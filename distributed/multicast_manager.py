import threading
from network.message import Message
from network.client import Client
from distributed.message_types import MessageType
from distributed.holdback_queue import HoldbackQueue
from managers.file_manager import FileManager


class MulticastManager:
    """
    Implements multicast communication supporting:
    - FIFO Ordering (via per-sender sequence numbers)
    - Causal Ordering (via Vector Clocks)
    - Total Ordering (via Sequencer / Leader-based global sequence numbering)
    """

    def __init__(self, node):
        self.node = node
        self._lock = threading.Lock()

        # FIFO sequence numbers
        self._send_sequence = 0
        self._expected_sequence = {}

        # Vector Clocks (Causal Ordering)
        # self.vector_clock: node_id -> integer
        self.vector_clock = {node.node_id: 0}
        self.causal_queue = []  # list of dicts: {"sender":, "timestamp":, "payload":}

        # Sequencer sequence numbers (Total Ordering)
        self._global_seq = 0
        self._expected_global_seq = 1
        self.total_queue = {}  # global_seq -> payload

        self.holdback = HoldbackQueue()

    def send_echo(self, command_input):
        node = self.node
        ordering = getattr(node, "multicast_ordering", "fifo")

        if ordering == "fifo":
            return self._send_fifo(command_input)
        elif ordering == "causal":
            return self._send_causal(command_input)
        elif ordering == "total":
            return self._send_total(command_input)
        return "Unknown ordering"

    # ------------------------------------------------------------------
    # FIFO Multicast
    # ------------------------------------------------------------------

    def _send_fifo(self, command_input):
        node = self.node
        self._send_sequence += 1
        sequence = self._send_sequence

        self._deliver(command_input)
        self._expected_sequence[node.node_id] = sequence + 1

        message = Message(
            MessageType.MULTICAST,
            sender=node.node_id,
            payload={"command": command_input, "ordering": "fifo"},
            sequence=sequence
        )

        for peer_id, info in list(node.peer_manager.peers.items()):
            threading.Thread(target=Client.send_to, args=(info["host"], info["port"], message), daemon=True).start()

        return "OK"

    # ------------------------------------------------------------------
    # Causal Multicast
    # ------------------------------------------------------------------

    def _send_causal(self, command_input):
        node = self.node
        with self._lock:
            # Sync vector clock keys with current peer group
            for pid in list(node.peer_manager.peers.keys()) + [node.node_id]:
                if pid not in self.vector_clock:
                    self.vector_clock[pid] = 0

            self.vector_clock[node.node_id] += 1
            timestamp = dict(self.vector_clock)

        self._deliver(command_input)

        message = Message(
            MessageType.MULTICAST,
            sender=node.node_id,
            payload={
                "command": command_input,
                "ordering": "causal",
                "timestamp": timestamp
            }
        )

        for peer_id, info in list(node.peer_manager.peers.items()):
            threading.Thread(target=Client.send_to, args=(info["host"], info["port"], message), daemon=True).start()

        return "OK"

    # ------------------------------------------------------------------
    # Total Multicast (Sequencer-based)
    # ------------------------------------------------------------------

    def _send_total(self, command_input):
        node = self.node
        leader = node.get_leader()

        if leader is None:
            # Single node: deliver immediately
            self._deliver(command_input)
            return "OK"

        if node.is_leader:
            # We are the sequencer: assign sequence and multicast
            with self._lock:
                self._global_seq += 1
                seq = self._global_seq

            # Deliver local
            self._deliver_total(seq, command_input)

            message = Message(
                MessageType.MULTICAST,
                sender=node.node_id,
                payload={
                    "command": command_input,
                    "ordering": "total",
                    "global_seq": seq
                }
            )

            for peer_id, info in list(node.peer_manager.peers.items()):
                threading.Thread(target=Client.send_to, args=(info["host"], info["port"], message), daemon=True).start()
        else:
            # Send request to sequencer (the leader)
            req_msg = Message(
                MessageType.TOTAL_SEQUENCER_REQUEST,
                sender=node.node_id,
                payload=command_input
            )
            threading.Thread(target=Client.send_to, args=(leader["host"], leader["port"], req_msg), daemon=True).start()

        return "OK"

    def handle_sequencer_request(self, message: Message):
        """
        Called on the leader (sequencer) node.
        """
        command = message.payload
        self._send_total(command)
        return Message(MessageType.RESPONSE, sender=self.node.node_id, payload="ACK")

    # ------------------------------------------------------------------
    # Message Dispatch / Handling
    # ------------------------------------------------------------------

    def handle_message(self, message: Message):
        sender = message.sender
        payload = message.payload

        ordering = payload.get("ordering", "fifo")

        if ordering == "fifo":
            sequence = message.sequence or 1
            expected = self._expected_sequence.get(sender, 1)

            if sequence == expected:
                self._deliver(payload["command"])
                self._expected_sequence[sender] = expected + 1
                self._flush_fifo_holdback(sender)
            elif sequence > expected:
                self.holdback.add(sender, sequence, payload["command"])
                print(f"Node {self.node.node_id}: FIFO holdback seq={sequence}, expected={expected}")

        elif ordering == "causal":
            timestamp = payload["timestamp"]
            with self._lock:
                # Sync vector clock keys
                for pid in [sender, self.node.node_id] + list(self.node.peer_manager.peers.keys()):
                    if pid not in self.vector_clock:
                        self.vector_clock[pid] = 0
                for pid in timestamp:
                    if pid not in self.vector_clock:
                        self.vector_clock[pid] = 0

                self.causal_queue.append({
                    "sender": sender,
                    "timestamp": timestamp,
                    "command": payload["command"]
                })
                self._flush_causal()

        elif ordering == "total":
            global_seq = payload["global_seq"]
            self._deliver_total(global_seq, payload["command"])

        return Message(
            MessageType.ACK,
            sender=self.node.node_id,
            payload="MULTICAST RECEIVED"
        )

    # ------------------------------------------------------------------
    # Flushes
    # ------------------------------------------------------------------

    def _flush_fifo_holdback(self, sender):
        expected = self._expected_sequence.get(sender, 1)
        while self.holdback.has(sender, expected):
            cmd = self.holdback.pop(sender, expected)
            self._deliver(cmd)
            expected += 1
            self._expected_sequence[sender] = expected

    def _flush_causal(self):
        delivered_any = True
        while delivered_any:
            delivered_any = False
            for idx, msg in enumerate(list(self.causal_queue)):
                sender = msg["sender"]
                W = msg["timestamp"]

                # Check conditions
                # 1. W[sender] == V[sender] + 1
                cond1 = (W[sender] == self.vector_clock[sender] + 1)
                # 2. for all k != sender, W[k] <= V[k]
                cond2 = True
                for k in W:
                    if k != sender:
                        if W[k] > self.vector_clock.get(k, 0):
                            cond2 = False
                            break

                if cond1 and cond2:
                    self._deliver(msg["command"])
                    self.vector_clock[sender] = W[sender]
                    self.causal_queue.pop(idx)
                    delivered_any = True
                    break

    def _deliver_total(self, seq, command):
        with self._lock:
            self.total_queue[seq] = command
            self._flush_total()

    def _flush_total(self):
        while self._expected_global_seq in self.total_queue:
            cmd = self.total_queue.pop(self._expected_global_seq)
            self._deliver(cmd)
            self._expected_global_seq += 1

    def _deliver(self, command_input):
        try:
            FileManager.echo(command_input)
            print(f"Node {self.node.node_id}: delivered multicast command -> {command_input}")
        except Exception as e:
            print(f"Node {self.node.node_id}: error applying multicast command: {e}")
