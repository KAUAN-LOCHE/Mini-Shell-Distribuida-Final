import time
import threading


class DeadlockDetector:
    """
    Implements deadlock detection using the Wait-For Graph (WFG).
    Runs a background thread that periodically checks for cycles in the WFG,
    identifies the youngest transaction in the cycle, and aborts it.
    """

    def __init__(self, node, check_interval=2.0):
        self.node = node
        self.check_interval = check_interval
        self._running = False
        self._thread = None

    def start(self):
        self._running = True
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        print(f"Node {self.node.node_id}: Deadlock detector started.")

    def stop(self):
        self._running = False

    def _loop(self):
        while self._running:
            time.sleep(self.check_interval)
            try:
                self.check_and_resolve_deadlocks()
            except Exception as e:
                print(f"Error in deadlock detector loop: {e}")

    def check_and_resolve_deadlocks(self):
        node = self.node
        lock_mgr = getattr(node, "lock_manager_2pl", None)
        if not lock_mgr:
            return

        wfg = None
        with lock_mgr._lock:
            # Copy WFG to avoid thread race issues while reading
            wfg = {tid: set(targets) for tid, targets in list(lock_mgr.wfg.items())}

        if not wfg:
            return

        cycle = self.find_cycle(wfg)
        if cycle:
            # Cycle detected! Choose the victim transaction (youngest: highest timestamp)
            print(f"Node {node.node_id}: deadlocks detected! Cycle path: {cycle}")
            victim = self.choose_victim(cycle)
            print(f"Node {node.node_id}: selecting victim {victim} for abort to break deadlock.")
            # Abort the victim locally
            node.transaction_manager._abort_local(victim)
            node.transaction_manager.write_log(f"{victim} GLOBAL_ABORT_DEADLOCK")

    def find_cycle(self, wfg):
        visited = set()
        rec_stack = set()
        path = []

        def dfs(u):
            visited.add(u)
            rec_stack.add(u)
            path.append(u)

            for v in wfg.get(u, []):
                if v not in visited:
                    if dfs(v):
                        return True
                elif v in rec_stack:
                    # Cycle found! Extract the cycle path starting at neighbor v
                    idx = path.index(v)
                    # Return only the nodes in the cycle
                    return path[idx:]

            path.pop()
            rec_stack.remove(u)
            return False

        for node in list(wfg.keys()):
            if node not in visited:
                cycle_path = []
                if dfs(node):
                    return path
        return None

    def choose_victim(self, cycle):
        # Transaction format: tx_<timestamp>_<node_id>
        # Younger transactions have larger timestamps.
        # Find the one with the maximum timestamp in the cycle list.
        def get_ts(tid):
            try:
                return int(tid.split("_")[1])
            except Exception:
                return 0

        return max(cycle, key=get_ts)
