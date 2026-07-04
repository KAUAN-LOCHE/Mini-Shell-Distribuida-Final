import threading


class Lock2PL:
    def __init__(self):
        self.lock_type = None  # 'READ', 'WRITE'
        self.owners = set()     # set of TIDs
        self.condition = threading.Condition()


class LockManager2PL:
    """
    Implements a Lock Manager supporting Shared (Read) and Exclusive (Write) locks,
    Strict 2-Phase Locking (2PL), and lock inheritance in nested transactions.
    """

    def __init__(self, node):
        self.node = node
        self._lock = threading.Lock()
        self.locks = {}  # resource -> Lock2PL
        
        # Transaction structure: TID -> parent_TID
        self.tx_parents = {}
        # Wait-For Graph: waiting_TID -> set of TIDs it is waiting for
        self.wfg = {}

    def register_transaction(self, tid, parent_tid=None):
        with self._lock:
            self.tx_parents[tid] = parent_tid

    def is_ancestor(self, ancestor_tid, desc_tid):
        """
        Returns True if ancestor_tid is a direct or indirect parent of desc_tid.
        """
        current = desc_tid
        while current is not None:
            parent = self.tx_parents.get(current)
            if parent == ancestor_tid:
                return True
            current = parent
        return False

    def acquire_lock(self, tid, resource, lock_type) -> bool:
        """
        Acquires a lock for a transaction (TID) on a resource.
        lock_type is 'READ' (Shared) or 'WRITE' (Exclusive).
        Returns True if acquired, False otherwise (or if it blocked and was aborted due to deadlock).
        """
        lock_obj = None
        with self._lock:
            if resource not in self.locks:
                self.locks[resource] = Lock2PL()
            lock_obj = self.locks[resource]

        with lock_obj.condition:
            while True:
                with self._lock:
                    # Check compatibility
                    conflict = False
                    other_owners = {owner for owner in lock_obj.owners if owner != tid}

                    if lock_obj.lock_type == 'WRITE':
                        # Write lock held by another transaction that is not an ancestor
                        for owner in other_owners:
                            if not self.is_ancestor(owner, tid):
                                conflict = True
                                break
                    elif lock_obj.lock_type == 'READ' and lock_type == 'WRITE':
                        # We want a Write lock but others hold Read locks.
                        # Conflict if any owner is not an ancestor of tid
                        for owner in other_owners:
                            if not self.is_ancestor(owner, tid):
                                conflict = True
                                break

                    if not conflict:
                        # Lock acquired/granted!
                        lock_obj.lock_type = lock_type
                        lock_obj.owners.add(tid)
                        # Remove from Wait-For Graph
                        if tid in self.wfg:
                            del self.wfg[tid]
                        print(f"Node {self.node.node_id}: lock {lock_type} granted to {tid} on '{resource}'")
                        return True

                    # Add edge to Wait-For Graph: tid is waiting for all owners of the lock
                    self.wfg[tid] = set(lock_obj.owners)
                    print(f"Node {self.node.node_id}: transaction {tid} waiting for lock on '{resource}' held by {list(lock_obj.owners)}")

                # Check if there is a deadlock (run deadlock detection on coordinator)
                # Wait for lock to be freed
                success = lock_obj.condition.wait(timeout=5.0)
                if not success:
                    # Timeout: abort transaction due to possible deadlock / timeout
                    print(f"Node {self.node.node_id}: transaction {tid} lock request timed out on '{resource}'")
                    with self._lock:
                        if tid in self.wfg:
                            del self.wfg[tid]
                    return False

    def release_locks(self, tid):
        """
        Releases all locks held by a transaction (typically on abort).
        """
        with self._lock:
            for resource, lock_obj in list(self.locks.items()):
                with lock_obj.condition:
                    if tid in lock_obj.owners:
                        lock_obj.owners.remove(tid)
                        if not lock_obj.owners:
                            lock_obj.lock_type = None
                        lock_obj.condition.notify_all()
            if tid in self.tx_parents:
                del self.tx_parents[tid]
            if tid in self.wfg:
                del self.wfg[tid]

    def inherit_locks(self, sub_tid, parent_tid):
        """
        Lock Inheritance: transfer all locks held by sub_tid to parent_tid (on commit).
        """
        with self._lock:
            for resource, lock_obj in list(self.locks.items()):
                with lock_obj.condition:
                    if sub_tid in lock_obj.owners:
                        lock_obj.owners.remove(sub_tid)
                        lock_obj.owners.add(parent_tid)
                        lock_obj.condition.notify_all()
            if sub_tid in self.tx_parents:
                del self.tx_parents[sub_tid]
            if sub_tid in self.wfg:
                del self.wfg[sub_tid]
            print(f"Node {self.node.node_id}: locks of subtransaction {sub_tid} inherited by parent {parent_tid}")
