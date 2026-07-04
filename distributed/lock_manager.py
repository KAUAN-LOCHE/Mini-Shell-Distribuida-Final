from network.message import Message
from distributed.message_types import MessageType


class LockManager:
    """
    Centralized mutual-exclusion manager. Runs on the current leader node
    and keeps a table of resource -> owner node_id.
    """

    def __init__(self, node):
        self.node = node

        # resource -> node_id
        self.lock_table = {}

    def handle_lock(self, message: Message):

        resource = message.payload

        owner = self.lock_table.get(resource)

        if owner is None or owner == message.sender:

            self.lock_table[resource] = message.sender

            return Message(
                MessageType.LOCK_GRANTED,
                sender=self.node.node_id,
                payload=resource
            )

        return Message(
            MessageType.LOCK_DENIED,
            sender=self.node.node_id,
            payload=resource
        )

    def handle_unlock(self, message: Message):

        resource = message.payload

        owner = self.lock_table.get(resource)

        if owner == message.sender:
            del self.lock_table[resource]

        return Message(
            MessageType.RESPONSE,
            sender=self.node.node_id,
            payload="UNLOCK OK"
        )
