from enum import Enum


class MessageType(Enum):

    COMMAND = "COMMAND"

    # Mutual exclusion (Centralized)
    LOCK = "LOCK"
    UNLOCK = "UNLOCK"
    LOCK_GRANTED = "LOCK_GRANTED"
    LOCK_DENIED = "LOCK_DENIED"

    # Ricart-Agrawala
    RICART_REQUEST = "RICART_REQUEST"
    RICART_REPLY = "RICART_REPLY"

    # Maekawa
    MAEKAWA_REQUEST = "MAEKAWA_REQUEST"
    MAEKAWA_VOTE = "MAEKAWA_VOTE"
    MAEKAWA_RELEASE = "MAEKAWA_RELEASE"

    # Election (Bully)
    ELECTION = "ELECTION"
    OK = "OK"
    COORDINATOR = "COORDINATOR"

    # Election (Ring)
    RING_ELECTION = "RING_ELECTION"
    RING_COORDINATOR = "RING_COORDINATOR"

    # Multicast messages (reliable / ordered multicast)
    MULTICAST = "MULTICAST"
    ACK = "ACK"
    TOTAL_SEQUENCER_REQUEST = "TOTAL_SEQUENCER_REQUEST"

    # Consensus
    CONSENSUS_PROPOSE = "CONSENSUS_PROPOSE"
    CONSENSUS_DECIDE = "CONSENSUS_DECIDE"

    # Distributed Transactions / 2PC
    TX_PREPARE = "TX_PREPARE"
    TX_VOTE_COMMIT = "TX_VOTE_COMMIT"
    TX_VOTE_ABORT = "TX_VOTE_ABORT"
    TX_GLOBAL_COMMIT = "TX_GLOBAL_COMMIT"
    TX_GLOBAL_ABORT = "TX_GLOBAL_ABORT"

    # Peer discovery
    PEER_JOIN = "PEER_JOIN"

    # Responses
    RESPONSE = "RESPONSE"

    # Fault simulation
    CRASH = "CRASH"

