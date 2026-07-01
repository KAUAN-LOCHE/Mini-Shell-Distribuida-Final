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

        elif command_input.startswith("ls"):
            ProcessManager.execute(command_input)

        else:
            return f"Unknown command: {command_input}"

        return "OK"

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
        leader = node.get_leader()

        if leader is None:
            # No leader known yet (e.g. isolated node): allow the operation.
            return True

        message = Message(MessageType.LOCK, sender=node.node_id, payload=resource)

        if node.leader_id == node.node_id:
            response = node.lock_manager.handle_lock(message)
        else:
            response = Client.send_to(leader["host"], leader["port"], message)

        return response is not None and response.type == MessageType.LOCK_GRANTED.value

    def _release_lock(self, resource):

        node = self.node
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
