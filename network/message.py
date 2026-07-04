import json
from distributed.message_types import MessageType


class Message:

    def __init__(
        self,
        message_type: MessageType,
        sender: int,
        payload=None,
        sequence=None
    ):
        self.type = message_type.value
        self.sender = sender
        self.payload = payload
        self.sequence = sequence

    def to_json(self):
        return json.dumps({
            "type": self.type,
            "sender": self.sender,
            "payload": self.payload,
            "sequence": self.sequence
        })

    @staticmethod
    def from_json(data):

        obj = json.loads(data)

        return Message(
            message_type=MessageType(obj["type"]),
            sender=obj["sender"],
            payload=obj.get("payload"),
            sequence=obj.get("sequence")
        )

    def __repr__(self):
        return f"Message(type={self.type}, sender={self.sender}, payload={self.payload}, sequence={self.sequence})"
