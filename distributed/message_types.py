from enum import Enum


class MessageType(Enum):

    COMMAND = "COMMAND"

    # Mutual exclusion
    LOCK = "LOCK"
    UNLOCK = "UNLOCK"
    LOCK_GRANTED = "LOCK_GRANTED"
    LOCK_DENIED = "LOCK_DENIED"

    # Bully algorithm
    ELECTION = "ELECTION"
    OK = "OK"
    COORDINATOR = "COORDINATOR"

    # Multicast messages (reliable / ordered multicast)
    MULTICAST = "MULTICAST"
    ACK = "ACK"

    # Peer discovery
    PEER_JOIN = "PEER_JOIN"

    # Responses
    RESPONSE = "RESPONSE"

    # Fault simulation
    CRASH = "CRASH"
