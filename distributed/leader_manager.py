import threading

from network.message import Message
from network.client import Client
from distributed.message_types import MessageType


class LeaderManager:
    """
    Implements the Bully algorithm for leader election.
    """

    ELECTION_TIMEOUT = 2.0

    def __init__(self, node):
        self.node = node
        self._election_lock = threading.Lock()

    def start_election(self):
        """
        Sends ELECTION to every peer with a higher id and waits (with a
        timeout) for OK replies. If nobody with a higher id answers,
        this node declares itself the new coordinator.
        """

        # Avoid two elections running at the same time on this node.
        if not self._election_lock.acquire(blocking=False):
            return

        try:
            node = self.node
            print(f"Node {node.node_id} starting an election (Bully algorithm).")

            higher_peers = node.peer_manager.peers_with_higher_id(node.node_id)

            received_ok = False

            for peer_id, info in higher_peers.items():
                message = Message(MessageType.ELECTION, sender=node.node_id, payload=None)

                response = Client.send_to(
                    info["host"], info["port"], message, timeout=self.ELECTION_TIMEOUT
                )

                if response is not None and response.type == MessageType.OK.value:
                    print(f"Node {node.node_id} received OK from node {peer_id}; yielding election.")
                    received_ok = True

            if not received_ok:
                self._declare_victory()

        finally:
            self._election_lock.release()

    def _declare_victory(self):
        node = self.node

        node.is_leader = True
        node.leader_id = node.node_id

        print(f"Node {node.node_id} won the election and is now the coordinator (LIDER).")

        for peer_id, info in list(node.peer_manager.peers.items()):
            message = Message(MessageType.COORDINATOR, sender=node.node_id, payload=node.node_id)
            Client.send_to(info["host"], info["port"], message, timeout=self.ELECTION_TIMEOUT)

    def handle_election(self, message: Message):
        """
        Called when this node receives an ELECTION message from a node
        with a lower id. Per the Bully algorithm: reply OK (so the
        sender backs off) and start our own election, since we outrank it.
        """

        node = self.node

        if not node.alive:
            return None

        print(f"Node {node.node_id} received ELECTION from node {message.sender}.")

        threading.Thread(target=self.start_election, daemon=True).start()

        return Message(MessageType.OK, sender=node.node_id, payload=None)

    def handle_coordinator(self, message: Message):
        """
        Called when this node is informed that `message.sender` is the
        new coordinator.
        """

        node = self.node

        node.leader_id = message.sender
        node.is_leader = (node.leader_id == node.node_id)

        print(f"Node {node.node_id} acknowledges node {message.sender} as the new coordinator (LIDER).")

        return Message(MessageType.RESPONSE, sender=node.node_id, payload="OK")

    def check_leader_alive(self):
        """
        Pings the current leader. If it doesn't answer, triggers a new
        election. Returns True if the leader responded (or if this node
        doesn't know of any leader/peers yet).
        """

        node = self.node

        if node.is_leader or node.leader_id is None:
            return True

        leader = node.get_leader()

        if leader is None:
            return True

        ping = Message(MessageType.RESPONSE, sender=node.node_id, payload="PING")

        response = Client.send_to(
            leader["host"], leader["port"], ping, timeout=self.ELECTION_TIMEOUT
        )

        if response is None:
            print(f"Node {node.node_id} detected that leader {node.leader_id} is unreachable.")
            self.start_election()
            return False

        return True
