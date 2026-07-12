from managers.process_manager import ProcessManager
from managers.file_manager import FileManager
from managers.thread_manager import ThreadManager
from network.message import Message
from network.client import Client
from distributed.message_types import MessageType


class CommandParser:

    def __init__(self, node):
        self.node = node

    def execute(self, command_input):

        command_input = command_input.strip()

        if command_input == "begin-tx":
            return self.node.transaction_manager.begin()
        elif command_input == "begin-subtx":
            return self.node.transaction_manager.begin()
        elif command_input == "commit-tx":
            return self.node.transaction_manager.commit()
        elif command_input == "abort-tx":
            return self.node.transaction_manager.abort()

        tx_res = self._execute_transactional(command_input)
        if tx_res is not None:
            return tx_res

        if command_input.startswith("mkdir"):
            return self._mkdir(command_input)

        elif command_input.startswith("rmdir -rf"):
            FileManager.remove_recursively(command_input)

        elif command_input.startswith("rmdir"):
            FileManager.rmdir(command_input)

        elif command_input.startswith("cd"):
            FileManager.cd(command_input)

        elif command_input.startswith("cp"):
            FileManager.cp(command_input)

        elif command_input.startswith("echo"):
            return self.node.multicast_manager.send_echo(command_input)

        elif command_input.startswith("backup-dir"):
            ThreadManager.backup(command_input)

        elif command_input.startswith("process-test"):
            ProcessManager.test()

        elif command_input.startswith("thread-test"):
            ThreadManager.test()

        elif command_input.startswith("lock-resource"):
            return self._lock_resource(command_input)

        elif command_input.startswith("unlock-resource"):
            return self._unlock_resource(command_input)

        elif command_input.startswith("connect"):
            return self._connect(command_input)

        elif command_input.startswith("peers"):
            return self._list_peers()

        elif command_input.startswith("elect"):
            self.node.leader_manager.start_election()
            return "Election started"

        elif command_input.startswith("crash"):
            self.node.crash()
            return "Node crashed (simulated)"

        elif command_input.startswith("revive"):
            self.node.revive()
            return "Node revived"

        elif command_input.startswith("election-algo"):
            parts = command_input.split()
            if len(parts) > 1:
                algo = parts[1].lower()
                if algo in ["bully", "ring"]:
                    self.node.election_algorithm = algo
                    return f"Election algorithm set to {algo}"
                return "Valid election algorithms: bully, ring"
            return f"Current election algorithm: {self.node.election_algorithm}"

        elif command_input.startswith("mutex-algo"):
            parts = command_input.split()
            if len(parts) > 1:
                algo = parts[1].lower()
                if algo in ["centralized", "ricart-agrawala", "maekawa"]:
                    self.node.mutex_algorithm = algo
                    return f"Mutex algorithm set to {algo}"
                return "Valid mutex algorithms: centralized, ricart-agrawala, maekawa"
            return f"Current mutex algorithm: {self.node.mutex_algorithm}"

        elif command_input.startswith("multicast-order"):
            parts = command_input.split()
            if len(parts) > 1:
                order = parts[1].lower()
                if order in ["fifo", "causal", "total"]:
                    self.node.multicast_ordering = order
                    return f"Multicast ordering set to {order}"
                return "Valid ordering types: fifo, causal, total"
            return f"Current multicast ordering: {self.node.multicast_ordering}"

        elif command_input.startswith("propose"):
            parts = command_input.split()
            if len(parts) > 1:
                val = parts[1]
                try:
                    val = float(val) if '.' in val else int(val)
                except ValueError:
                    pass
                return self.node.consensus_manager.propose(val)
            return "Usage: propose <value>"

        elif command_input.startswith("ls"):
            ProcessManager.execute(command_input)



        else:
            return f"Unknown command: {command_input}"

        return ""

    # ------------------------------------------------------------------
    # Mutual exclusion
    # ------------------------------------------------------------------

    def _mkdir(self, command_input):

        resource = command_input.split()[1]

        if self._acquire_lock(resource):
            FileManager.mkdir(command_input)
            self._release_lock(resource)
            return "Directory created"

        return "Resource busy."

    def _lock_resource(self, command_input):

        resource = command_input.split()[1]

        return "Lock acquired" if self._acquire_lock(resource) else "Resource busy."

    def _unlock_resource(self, command_input):

        resource = command_input.split()[1]

        self._release_lock(resource)

        return "Lock released"

    def _acquire_lock(self, resource):

        node = self.node
        algo = getattr(node, "mutex_algorithm", "centralized")

        if algo == "ricart-agrawala":
            return node.mutex_manager.acquire_ricart(resource)
        elif algo == "maekawa":
            return node.mutex_manager.acquire_maekawa(resource)

        # Centralized fallback
        leader = node.get_leader()
        if leader is None:
            return True

        message = Message(MessageType.LOCK, sender=node.node_id, payload=resource)

        if node.leader_id == node.node_id:
            response = node.lock_manager.handle_lock(message)
        else:
            response = Client.send_to(leader["host"], leader["port"], message)

        return response is not None and response.type == MessageType.LOCK_GRANTED.value

    def _release_lock(self, resource):

        node = self.node
        algo = getattr(node, "mutex_algorithm", "centralized")

        if algo == "ricart-agrawala":
            node.mutex_manager.release_ricart(resource)
            return
        elif algo == "maekawa":
            node.mutex_manager.release_maekawa(resource)
            return

        # Centralized fallback
        leader = node.get_leader()
        if leader is None:
            return

        message = Message(MessageType.UNLOCK, sender=node.node_id, payload=resource)

        if node.leader_id == node.node_id:
            node.lock_manager.handle_unlock(message)
        else:
            Client.send_to(leader["host"], leader["port"], message)


    # ------------------------------------------------------------------
    # Peer management / election
    # ------------------------------------------------------------------

    def _connect(self, command_input):

        parts = command_input.split()

        if len(parts) != 4:
            return "Usage: connect <id> <host> <port>"

        _, peer_id, host, port = parts

        try:
            peer_id = int(peer_id)
            port = int(port)
        except ValueError:
            return "Usage: connect <id> <host> <port>"

        self.node.add_peer(peer_id, host, port)

        join_message = Message(
            MessageType.PEER_JOIN,
            sender=self.node.node_id,
            payload={"host": self.node.host, "port": self.node.port}
        )

        Client.send_to(host, port, join_message)

        # A new node joined the group: re-run the election so the
        # correct coordinator (highest id) is (re)established.
        self.node.leader_manager.start_election()

        return "Connected"

    def _list_peers(self):

        peers = self.node.peer_manager.peers

        if not peers:
            return "No peers connected."

        lines = [
            f"Node {pid} -> {info['host']}:{info['port']}"
            for pid, info in peers.items()
        ]

        return "\n".join(lines)

    def _execute_transactional(self, command_input):
        import os
        tx_stack = self.node.transaction_manager.tx_stack
        if not tx_stack:
            return None

        tid = tx_stack[-1]
        scratch_dir = self.node.transaction_manager.active_tx_info[tid]["tentative_dir"]

        resource = None
        action = None

        if command_input.startswith("mkdir"):
            resource = command_input.split()[1]
            action = "mkdir"
        elif command_input.startswith("rmdir -rf"):
            resource = command_input.split()[2]
            action = "rmdir -rf"
        elif command_input.startswith("rmdir"):
            resource = command_input.split()[1]
            action = "rmdir"
        elif command_input.startswith("cp"):
            parts = command_input.split()
            if len(parts) >= 3:
                source = parts[1]
                dest = parts[2]
                if not self.node.lock_manager_2pl.acquire_lock(tid, source, 'READ'):
                    return f"Transaction {tid} aborted due to lock conflict on '{source}'"
                resource = dest
                action = "cp"
        elif command_input.startswith("echo"):
            if ">" in command_input:
                resource = command_input.split(">")[1].strip()
                action = "echo"

        if resource is None:
            return None

        # Acquire exclusive write lock
        if not self.node.lock_manager_2pl.acquire_lock(tid, resource, 'WRITE'):
            self.node.transaction_manager._abort_local(tid)
            self.node.transaction_manager.write_log(f"{tid} ABORT")
            tx_stack.remove(tid)
            return f"Transaction {tid} aborted due to lock conflict on '{resource}'"

        self.node.transaction_manager.write_log(f"{tid} WRITE {resource}")

        if action == "mkdir":
            FileManager.mkdir(command_input, scratch_dir=scratch_dir)
        elif action == "rmdir -rf":
            FileManager.remove_recursively(command_input, scratch_dir=scratch_dir)
        elif action == "rmdir":
            FileManager.rmdir(command_input, scratch_dir=scratch_dir)
        elif action == "cp":
            FileManager.cp(command_input, scratch_dir=scratch_dir)
        elif action == "echo":
            FileManager.echo(command_input, scratch_dir=scratch_dir)

        return f"Transaction {tid}: operation '{action}' executed in tentative scratch space."

