import threading
import time

from network.message import Message
from network.client import Client
from distributed.message_types import MessageType


class LeaderManager:
    """
    Implements leader election using either the Bully algorithm or
    the Chang-Roberts Ring algorithm.
    """

    ELECTION_TIMEOUT = 2.0

    def __init__(self, node):
        self.node = node
        self._election_lock = threading.Lock()
        self.participating = False

    def start_election(self):
        """
        Triggers leader election using the configured algorithm.
        """
        node = self.node
        if getattr(node, "election_algorithm", "bully") == "ring":
            threading.Thread(target=self.start_ring_election, daemon=True).start()
        else:
            threading.Thread(target=self.start_bully_election, daemon=True).start()

    def start_bully_election(self):
        """
        Sends ELECTION to every peer with a higher id and waits (with a
        timeout) for OK replies. If nobody with a higher id answers,
        this node declares itself the new coordinator.
        """
        if not self._election_lock.acquire(blocking=False):
            return

        try:
            node = self.node
            print(f"Node {node.node_id} starting an election (Bully algorithm).")

            higher_peers = node.peer_manager.peers_with_higher_id(node.node_id)
            received_ok = False

            for peer_id, info in list(higher_peers.items()):
                message = Message(MessageType.ELECTION, sender=node.node_id, payload=None)
                response = Client.send_to(
                    info["host"], info["port"], message, timeout=self.ELECTION_TIMEOUT
                )

                if response is not None and response.type == MessageType.OK.value:
                    print(f"Node {node.node_id} received OK from node {peer_id}; yielding election.")
                    received_ok = True

            if not received_ok:
                self._declare_bully_victory()

        finally:
            self._election_lock.release()

    def _declare_bully_victory(self):
        node = self.node
        node.is_leader = True
        node.leader_id = node.node_id

        print(f"Node {node.node_id} won the election and is now the coordinator (LIDER).")

        for peer_id, info in list(node.peer_manager.peers.items()):
            message = Message(MessageType.COORDINATOR, sender=node.node_id, payload=node.node_id)
            Client.send_to(info["host"], info["port"], message, timeout=self.ELECTION_TIMEOUT)

    def handle_election(self, message: Message):
        node = self.node
        if not node.alive:
            return None

        print(f"Node {node.node_id} received ELECTION from node {message.sender}.")
        threading.Thread(target=self.start_bully_election, daemon=True).start()
        return Message(MessageType.OK, sender=node.node_id, payload=None)

    def handle_coordinator(self, message: Message):
        node = self.node
        node.leader_id = message.sender
        node.is_leader = (node.leader_id == node.node_id)
        print(f"Node {node.node_id} acknowledges node {message.sender} as the new coordinator (LIDER).")
        return Message(MessageType.RESPONSE, sender=node.node_id, payload="OK")

    # ------------------------------------------------------------------
    # Chang-Roberts Ring Algorithm
    # ------------------------------------------------------------------

    def start_ring_election(self):
        """
        Starts a Chang-Roberts ring election.
        """
        node = self.node
        print(f"Node {node.node_id} starting a ring election (Chang-Roberts).")
        self.participating = True
        message = Message(
            MessageType.RING_ELECTION,
            sender=node.node_id,
            payload={"max_id": node.node_id}
        )
        self._send_to_successor(message)

    def handle_ring_election(self, message: Message):
        node = self.node
        if not node.alive:
            return None

        max_id = message.payload["max_id"]
        print(f"Node {node.node_id} received RING_ELECTION (max_id={max_id}).")

        if max_id > node.node_id:
            self.participating = True
            msg = Message(MessageType.RING_ELECTION, sender=node.node_id, payload={"max_id": max_id})
            threading.Thread(target=self._send_to_successor, args=(msg,), daemon=True).start()
        elif max_id < node.node_id:
            if not self.participating:
                self.participating = True
                msg = Message(MessageType.RING_ELECTION, sender=node.node_id, payload={"max_id": node.node_id})
                threading.Thread(target=self._send_to_successor, args=(msg,), daemon=True).start()
        else:  # max_id == node.node_id
            self._declare_ring_victory()

        return Message(MessageType.RESPONSE, sender=node.node_id, payload="OK")

    def _declare_ring_victory(self):
        node = self.node
        node.is_leader = True
        node.leader_id = node.node_id
        self.participating = False

        print(f"Node {node.node_id} won the ring election and is now coordinator (LIDER).")

        message = Message(
            MessageType.RING_COORDINATOR,
            sender=node.node_id,
            payload={"leader_id": node.node_id}
        )
        threading.Thread(target=self._send_to_successor, args=(message,), daemon=True).start()

    def handle_ring_coordinator(self, message: Message):
        node = self.node
        if not node.alive:
            return None

        leader_id = message.payload["leader_id"]
        print(f"Node {node.node_id} received RING_COORDINATOR (leader_id={leader_id}).")

        if leader_id != node.node_id:
            node.leader_id = leader_id
            node.is_leader = False
            self.participating = False
            msg = Message(MessageType.RING_COORDINATOR, sender=node.node_id, payload={"leader_id": leader_id})
            threading.Thread(target=self._send_to_successor, args=(msg,), daemon=True).start()
        else:
            print(f"Node {node.node_id} ring election complete.")

        return Message(MessageType.RESPONSE, sender=node.node_id, payload="OK")

    def _send_to_successor(self, message):
        node = self.node
        nodes = sorted([node.node_id] + list(node.peer_manager.peers.keys()))
        if len(nodes) <= 1:
            if message.type == MessageType.RING_ELECTION.value:
                self._declare_ring_victory()
            return

        my_idx = nodes.index(node.node_id)

        for i in range(1, len(nodes)):
            target_idx = (my_idx + i) % len(nodes)
            target_id = nodes[target_idx]
            if target_id == node.node_id:
                break

            info = node.peer_manager.get_peer(target_id)
            if info:
                res = Client.send_to(info["host"], info["port"], message, timeout=1.0)
                if res is not None:
                    return

        # If no successor could be contacted, we are alone
        if message.type == MessageType.RING_ELECTION.value:
            self._declare_ring_victory()

    # ------------------------------------------------------------------
    # Health checks
    # ------------------------------------------------------------------

    def check_leader_alive(self):
        """
        Pings the current leader. If it doesn't answer, triggers a new election.
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

