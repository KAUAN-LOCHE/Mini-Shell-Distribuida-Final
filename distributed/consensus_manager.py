import threading
import time
from network.message import Message
from network.client import Client
from distributed.message_types import MessageType


class ConsensusManager:
    """
    Implements a consensus protocol:
    - Nodes propose a value (e.g., a number or command).
    - The coordinator collects proposals from all alive nodes.
    - The coordinator makes a deterministic choice (e.g., majority or min).
    - The choice is multicast to all participants as the final decision.
    """

    def __init__(self, node):
        self.node = node
        self._lock = threading.Lock()

        # Consensus state on the leader
        self.proposals = {}  # proposal_id -> {node_id: value}
        self.decision = {}  # proposal_id -> decided_value
        self.proposal_conditions = {}  # proposal_id -> Condition

    def propose(self, value):
        """
        Called when the user runs the `propose <value>` command.
        """
        node = self.node
        leader = node.get_leader()

        proposal_id = f"{int(time.time())}_{node.node_id}"

        if leader is None:
            print(f"Node {node.node_id}: Consensus decided immediately (no peers) -> {value}")
            return f"Decision: {value}"

        if node.is_leader:
            # We are the coordinator: start the consensus collection
            threading.Thread(target=self._run_consensus, args=(proposal_id, node.node_id, value), daemon=True).start()
        else:
            # Forward the proposal to the coordinator
            msg = Message(
                MessageType.CONSENSUS_PROPOSE,
                sender=node.node_id,
                payload={"proposal_id": proposal_id, "value": value}
            )
            threading.Thread(target=Client.send_to, args=(leader["host"], leader["port"], msg), daemon=True).start()

        return f"Proposal {proposal_id} submitted. Awaiting decision..."

    def _run_consensus(self, proposal_id, proposer_id, value):
        node = self.node

        cond = threading.Condition()
        with self._lock:
            self.proposals[proposal_id] = {proposer_id: value}
            self.proposal_conditions[proposal_id] = cond

        # Request proposals from all peers
        msg = Message(
            MessageType.CONSENSUS_PROPOSE,
            sender=node.node_id,
            payload={"proposal_id": proposal_id, "request": True}
        )

        for pid, info in list(node.peer_manager.peers.items()):
            threading.Thread(target=Client.send_to, args=(info["host"], info["port"], msg), daemon=True).start()

        # Wait for all alive nodes to send their proposals (or timeout)
        with cond:
            def got_all_proposals():
                alive_peers = set(node.peer_manager.peers.keys()) | {node.node_id}
                received = set(self.proposals[proposal_id].keys())
                return alive_peers.issubset(received)

            cond.wait_for(got_all_proposals, timeout=4.0)

        # Decide
        with self._lock:
            received_vals = list(self.proposals[proposal_id].values())
            # Consensus function: choose the minimum value (or majority if string)
            # Choosing min is standard for distributed systems (e.g. smallest lexicographical or numeric value)
            if all(isinstance(v, (int, float)) for v in received_vals) or all(isinstance(v, str) for v in received_vals):
                decided = min(received_vals)
            else:
                decided = str(received_vals[0])

            self.decision[proposal_id] = decided

        print(f"Node {node.node_id} (COORDINATOR): Decided consensus on {proposal_id} -> {decided}")

        # Multicast decision
        decide_msg = Message(
            MessageType.CONSENSUS_DECIDE,
            sender=node.node_id,
            payload={"proposal_id": proposal_id, "decision": decided}
        )

        for pid, info in list(node.peer_manager.peers.items()):
            threading.Thread(target=Client.send_to, args=(info["host"], info["port"], decide_msg), daemon=True).start()

        # Apply locally
        self.handle_decision(decide_msg)

    def handle_proposal(self, message: Message):
        """
        Called when a node receives CONSENSUS_PROPOSE.
        """
        node = self.node
        payload = message.payload
        proposal_id = payload["proposal_id"]

        if payload.get("request"):
            # The coordinator is collecting proposals. We reply with our own proposal (or a dummy if we don't care).
            # We can propose self.node.node_id or a random number for demonstration.
            import random
            my_proposal = f"val_{node.node_id}"
            reply_msg = Message(
                MessageType.CONSENSUS_PROPOSE,
                sender=node.node_id,
                payload={"proposal_id": proposal_id, "value": my_proposal}
            )
            # Send back to coordinator
            leader = node.get_leader()
            if leader:
                threading.Thread(target=Client.send_to, args=(leader["host"], leader["port"], reply_msg), daemon=True).start()
        else:
            # We received a value proposal from a node
            val = payload["value"]
            if node.is_leader:
                with self._lock:
                    if proposal_id not in self.proposals:
                        self.proposals[proposal_id] = {}
                    self.proposals[proposal_id][message.sender] = val
                    cond = self.proposal_conditions.get(proposal_id)
                    if cond:
                        with cond:
                            cond.notify_all()
            else:
                # Forward to leader
                leader = node.get_leader()
                if leader:
                    threading.Thread(target=Client.send_to, args=(leader["host"], leader["port"], message), daemon=True).start()

        return Message(MessageType.RESPONSE, sender=node.node_id, payload="PROPOSAL RECEIVED")

    def handle_decision(self, message: Message):
        payload = message.payload
        proposal_id = payload["proposal_id"]
        decision = payload["decision"]

        print(f"*** CONSENSUS DECISION FOR {proposal_id}: '{decision}' ***")
        return Message(MessageType.RESPONSE, sender=self.node.node_id, payload="DECISION ACK")
