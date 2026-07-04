import os
import shutil
import time
import threading
from network.message import Message
from network.client import Client
from distributed.message_types import MessageType


class TransactionManager:
    """
    Manages Distributed Transactions:
    - Handles transaction stacks (including nested transactions).
    - Operates Tentative Versions (copy-on-write scratch areas).
    - Implements persistent logging (Write-Ahead Logging).
    - Executes the 2-Phase Commit (2PC) protocol.
    - Recovers state (REDO/UNDO) upon node revival.
    """

    def __init__(self, node):
        self.node = node
        self.tx_stack = []  # Stack of active transaction TIDs (top is current subtransaction)
        self.active_tx_info = {}  # TID -> {"parent":, "tentative_dir":, "status":}
        
        self.log_file = f"transaction_log_{node.node_id}.log"
        self._log_lock = threading.Lock()

        # Initialize log if not exists
        if not os.path.exists(self.log_file):
            with open(self.log_file, "w") as f:
                f.write("")

        # Lock manager shortcut
        self.lock_mgr = node.lock_manager  # Note: parser will map this to lock_manager_2pl

    def write_log(self, entry):
        """
        Appends an entry to the transaction log and flushes to disk.
        """
        with self._log_lock:
            with open(self.log_file, "a") as f:
                f.write(entry + "\n")
                f.flush()
                os.fsync(f.fileno())

    # ------------------------------------------------------------------
    # Shell Commands API
    # ------------------------------------------------------------------

    def begin(self) -> str:
        tid = f"tx_{int(time.time() * 1000)}_{self.node.node_id}"
        parent_tid = self.tx_stack[-1] if self.tx_stack else None

        tentative_dir = f"tx_scratch_{tid}"
        os.makedirs(tentative_dir, exist_ok=True)

        self.tx_stack.append(tid)
        self.active_tx_info[tid] = {
            "parent": parent_tid,
            "tentative_dir": tentative_dir,
            "status": "ACTIVE"
        }

        # Register in 2PL manager
        self.node.lock_manager_2pl.register_transaction(tid, parent_tid)

        if parent_tid:
            self.write_log(f"{tid} START_SUBTX parent={parent_tid}")
            # Inherit parent's tentative files (copy-on-write start)
            parent_dir = self.active_tx_info[parent_tid]["tentative_dir"]
            for item in os.listdir(parent_dir):
                shutil.copy2(os.path.join(parent_dir, item), tentative_dir)
            return f"Subtransaction {tid} started (parent: {parent_tid})"
        
        self.write_log(f"{tid} START")
        return f"Transaction {tid} started"

    def abort(self) -> str:
        if not self.tx_stack:
            return "No active transaction."
        tid = self.tx_stack.pop()
        
        self._abort_local(tid)
        
        # If it was a subtransaction
        parent = self.active_tx_info[tid]["parent"]
        self.write_log(f"{tid} ABORT")
        
        return f"Transaction {tid} aborted and rolled back."

    def commit(self) -> str:
        if not self.tx_stack:
            return "No active transaction."
        tid = self.tx_stack[-1]

        parent_tid = self.active_tx_info[tid]["parent"]
        if parent_tid:
            # Subtransaction commit: local inheritance
            self.tx_stack.pop()
            self.active_tx_info[tid]["status"] = "COMMITTED"
            self.write_log(f"{tid} COMMIT_SUBTX")

            # Merge subtransaction tentative files into parent tentative folder
            sub_dir = self.active_tx_info[tid]["tentative_dir"]
            parent_dir = self.active_tx_info[parent_tid]["tentative_dir"]
            for item in os.listdir(sub_dir):
                shutil.copy2(os.path.join(sub_dir, item), parent_dir)
            
            # Clean subtransaction tentative folder
            shutil.rmtree(sub_dir, ignore_errors=True)

            # Subtransaction locks are inherited by parent
            self.node.lock_manager_2pl.inherit_locks(tid, parent_tid)
            return f"Subtransaction {tid} committed provisionally into parent {parent_tid}."

        # Top-level commit: Run 2PC protocol
        self.tx_stack.pop()
        success = self._run_2pc(tid)
        if success:
            return f"Transaction {tid} committed successfully globally."
        else:
            return f"Transaction {tid} aborted globally (2PC failed)."

    # ------------------------------------------------------------------
    # Local Application & Rollback
    # ------------------------------------------------------------------

    def _apply_local(self, tid):
        info = self.active_tx_info.get(tid)
        if not info:
            return
        
        tentative_dir = info["tentative_dir"]
        # Copy tentative files to the actual directory (overwrite)
        if os.path.exists(tentative_dir):
            for item in os.listdir(tentative_dir):
                src = os.path.join(tentative_dir, item)
                if os.path.isfile(src):
                    shutil.copy2(src, ".")
                elif os.path.isdir(src):
                    # For directories, recreate them in the workspace
                    os.makedirs(item, exist_ok=True)
            shutil.rmtree(tentative_dir, ignore_errors=True)

        self.node.lock_manager_2pl.release_locks(tid)
        if tid in self.active_tx_info:
            self.active_tx_info[tid]["status"] = "COMMITTED"

    def _abort_local(self, tid):
        info = self.active_tx_info.get(tid)
        if not info:
            return
        
        tentative_dir = info["tentative_dir"]
        # Discard tentative directory
        shutil.rmtree(tentative_dir, ignore_errors=True)

        self.node.lock_manager_2pl.release_locks(tid)
        if tid in self.active_tx_info:
            self.active_tx_info[tid]["status"] = "ABORTED"

    # ------------------------------------------------------------------
    # Two-Phase Commit (2PC) Protocol
    # ------------------------------------------------------------------

    def _run_2pc(self, tid) -> bool:
        node = self.node
        print(f"Node {node.node_id} (COORDINATOR): initiating 2PC for {tid}")
        self.write_log(f"{tid} PREPARE")

        prepare_msg = Message(
            MessageType.TX_PREPARE,
            sender=node.node_id,
            payload={"tid": tid}
        )

        votes = {}
        expected_votes = set(node.peer_manager.peers.keys())

        # Multicast prepare
        for pid, info in list(node.peer_manager.peers.items()):
            response = Client.send_to(info["host"], info["port"], prepare_msg, timeout=2.0)
            if response and response.type == MessageType.TX_VOTE_COMMIT.value:
                votes[pid] = "COMMIT"
            else:
                votes[pid] = "ABORT"

        # Check votes
        all_commit = True
        for pid in expected_votes:
            if votes.get(pid) != "COMMIT":
                all_commit = False
                break

        if all_commit:
            # Global Commit decision
            self.write_log(f"{tid} GLOBAL_COMMIT")
            print(f"Node {node.node_id} (COORDINATOR): 2PC success for {tid}. Committing...")

            commit_msg = Message(
                MessageType.TX_GLOBAL_COMMIT,
                sender=node.node_id,
                payload={"tid": tid}
            )
            for pid, info in list(node.peer_manager.peers.items()):
                threading.Thread(target=Client.send_to, args=(info["host"], info["port"], commit_msg), daemon=True).start()

            self._apply_local(tid)
            return True
        else:
            # Global Abort decision
            self.write_log(f"{tid} GLOBAL_ABORT")
            print(f"Node {node.node_id} (COORDINATOR): 2PC failed/aborted for {tid}.")

            abort_msg = Message(
                MessageType.TX_GLOBAL_ABORT,
                sender=node.node_id,
                payload={"tid": tid}
            )
            for pid, info in list(node.peer_manager.peers.items()):
                threading.Thread(target=Client.send_to, args=(info["host"], info["port"], abort_msg), daemon=True).start()

            self._abort_local(tid)
            return False

    def handle_prepare(self, message: Message):
        node = self.node
        tid = message.payload["tid"]
        print(f"Node {node.node_id}: received 2PC PREPARE for {tid}")

        # Check if we can commit: we are alive and have locks
        if node.alive:
            self.write_log(f"{tid} PREPARE")
            return Message(
                MessageType.TX_VOTE_COMMIT,
                sender=node.node_id,
                payload={"tid": tid}
            )
        else:
            self.write_log(f"{tid} ABORT")
            return Message(
                MessageType.TX_VOTE_ABORT,
                sender=node.node_id,
                payload={"tid": tid}
            )

    def handle_global_commit(self, message: Message):
        tid = message.payload["tid"]
        print(f"Node {self.node.node_id}: received 2PC GLOBAL_COMMIT for {tid}")
        self.write_log(f"{tid} GLOBAL_COMMIT")
        self._apply_local(tid)
        return Message(MessageType.RESPONSE, sender=self.node.node_id, payload="COMMIT_ACK")

    def handle_global_abort(self, message: Message):
        tid = message.payload["tid"]
        print(f"Node {self.node.node_id}: received 2PC GLOBAL_ABORT for {tid}")
        self.write_log(f"{tid} GLOBAL_ABORT")
        self._abort_local(tid)
        return Message(MessageType.RESPONSE, sender=self.node.node_id, payload="ABORT_ACK")

    # ------------------------------------------------------------------
    # Recovery (REDO / UNDO)
    # ------------------------------------------------------------------

    def recover(self):
        """
        Parses transaction log, executing REDO on committed transactions
        and UNDO on uncommitted ones.
        """
        if not os.path.exists(self.log_file):
            return

        print(f"Node {self.node.node_id}: running transaction recovery...")

        # Parse log file
        tx_states = {}  # TID -> final status: "STARTED", "PREPARED", "COMMITTED", "ABORTED"
        
        with open(self.log_file, "r") as f:
            for line in f:
                parts = line.strip().split()
                if not parts:
                    continue
                tid = parts[0]
                action = parts[1]

                if action == "START":
                    tx_states[tid] = "STARTED"
                elif action == "PREPARE":
                    tx_states[tid] = "PREPARED"
                elif action == "GLOBAL_COMMIT":
                    tx_states[tid] = "COMMITTED"
                elif action == "GLOBAL_ABORT" or action == "ABORT":
                    tx_states[tid] = "ABORTED"

        # Apply REDO / UNDO based on states
        for tid, state in tx_states.items():
            tentative_dir = f"tx_scratch_{tid}"
            if state == "COMMITTED":
                # REDO: make sure all changes from scratch are copied to real folders
                if os.path.exists(tentative_dir):
                    print(f"Node {self.node.node_id} Recovery: REDO transaction {tid}")
                    # Reuse local apply
                    self.active_tx_info[tid] = {"tentative_dir": tentative_dir}
                    self._apply_local(tid)
            elif state in ["STARTED", "PREPARED", "ABORTED"]:
                # UNDO: discard tentative files
                if os.path.exists(tentative_dir):
                    print(f"Node {self.node.node_id} Recovery: UNDO transaction {tid}")
                    self.active_tx_info[tid] = {"tentative_dir": tentative_dir}
                    self._abort_local(tid)

        print(f"Node {self.node.node_id}: transaction recovery complete.")
