from network.message import Message
from network.client import Client
from distributed.message_types import MessageType
from distributed.holdback_queue import HoldbackQueue
from managers.file_manager import FileManager


class MulticastManager:
    """
    Implements a simple R-multicast (reliable, ordered multicast):
    each node keeps its own outgoing sequence number, and every receiver
    keeps, per sender, the next sequence number it expects to deliver.
    Messages that arrive out of order are held in a HoldbackQueue until
    the missing ones show up.
    """

    def __init__(self, node):
        self.node = node

        self._send_sequence = 0

        # sender_id -> next expected sequence number
        self._expected_sequence = {}

        self.holdback = HoldbackQueue()

    def send_echo(self, command_input):
        """
        Called locally when the user types `echo "..." > file`.
        Applies the command locally and propagates it, in order, to
        every known peer.
        """

        node = self.node

        self._send_sequence += 1
        sequence = self._send_sequence

        self._deliver(command_input)
        self._expected_sequence[node.node_id] = sequence + 1

        message = Message(
            MessageType.MULTICAST,
            sender=node.node_id,
            payload=command_input,
            sequence=sequence
        )

        for peer_id, info in list(node.peer_manager.peers.items()):
            Client.send_to(info["host"], info["port"], message)

        return "OK"

    def handle_message(self, message: Message):
        """
        Called on the receiving side when a MULTICAST message arrives.
        """

        sender = message.sender
        sequence = message.sequence or 1

        expected = self._expected_sequence.get(sender, 1)

        if sequence == expected:
            self._deliver(message.payload)
            self._expected_sequence[sender] = expected + 1
            self._flush_holdback(sender)

        elif sequence > expected:
            self.holdback.add(sender, sequence, message.payload)
            print(
                f"Node {self.node.node_id}: multicast from {sender} arrived "
                f"out of order (seq={sequence}, expected={expected}); held back."
            )

        # sequence < expected -> duplicate/old message, ignore it

        return Message(
            MessageType.ACK,
            sender=self.node.node_id,
            payload=sequence
        )

    def _flush_holdback(self, sender):
        expected = self._expected_sequence.get(sender, 1)

        while self.holdback.has(sender, expected):
            payload = self.holdback.pop(sender, expected)
            self._deliver(payload)
            expected += 1
            self._expected_sequence[sender] = expected

    def _deliver(self, command_input):
        try:
            FileManager.echo(command_input)
            print(f"Node {self.node.node_id}: delivered multicast command -> {command_input}")
        except Exception as e:
            print(f"Node {self.node.node_id}: error applying multicast command: {e}")
