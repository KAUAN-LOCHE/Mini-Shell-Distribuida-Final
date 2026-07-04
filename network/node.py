import threading
import time

from network.server import Server
from network.peer_manager import PeerManager
from distributed.lock_manager import LockManager
from distributed.leader_manager import LeaderManager
from distributed.multicast_manager import MulticastManager
from distributed.mutex_manager import MutexManager
from distributed.consensus_manager import ConsensusManager
from distributed.lock_manager_2pl import LockManager2PL
from distributed.transaction_manager import TransactionManager
from distributed.deadlock_detector import DeadlockDetector
from shell.parser import CommandParser


class Node:

    HEARTBEAT_INTERVAL = 5.0

    def __init__(self, node_id: int, port: int, host: str = '127.0.0.1'):
        self.node_id = node_id
        self.port = port
        self.host = host

        self.server = None

        # Whether this node is currently "up". Set to False by the
        # `crash` shell command to simulate a failure: the server stops
        # answering to any incoming message.
        self.alive = True

        self.is_leader = False
        self.leader_id = None

        self.election_algorithm = "bully"
        self.mutex_algorithm = "centralized"
        self.multicast_ordering = "fifo"
        
        self.lamport_clock = 0
        self.lamport_lock = threading.Lock()

        self.peer_manager = PeerManager(self)
        self.lock_manager = LockManager(self)
        self.leader_manager = LeaderManager(self)
        self.multicast_manager = MulticastManager(self)
        self.mutex_manager = MutexManager(self)
        self.consensus_manager = ConsensusManager(self)
        self.lock_manager_2pl = LockManager2PL(self)
        self.transaction_manager = TransactionManager(self)
        self.deadlock_detector = DeadlockDetector(self)
        self.parser = CommandParser(self)


    def increment_lamport_clock(self, incoming_timestamp=0):
        with self.lamport_lock:
            self.lamport_clock = max(self.lamport_clock, incoming_timestamp) + 1
            return self.lamport_clock

    def start(self):
        self.server = Server(self.host, self.port, node=self)
        self.server.start()

        print(f"Node {self.node_id} started on {self.host}:{self.port}")

        # Every node starts out as its own leader; once it connects to
        # other nodes, a Bully election decides the real coordinator.
        self.is_leader = True
        self.leader_id = self.node_id

        # Start Deadlock Detector
        self.deadlock_detector.start()
        # Recover transactions
        self.transaction_manager.recover()

        self._start_heartbeat()

    def _start_heartbeat(self):

        def loop():
            while True:
                time.sleep(self.HEARTBEAT_INTERVAL)

                if self.alive and not self.is_leader and len(self.peer_manager) > 0:
                    self.leader_manager.check_leader_alive()

        threading.Thread(target=loop, daemon=True).start()

    def add_peer(self, node_id, host, port):
        self.peer_manager.add_peer(node_id, host, port)
        print(f"Node {self.node_id} added peer {node_id} at {host}:{port}")

    def remove_peer(self, node_id):
        if node_id in self.peer_manager:
            self.peer_manager.remove_peer(node_id)
            print(f"Node {self.node_id} removed peer {node_id}")
        else:
            print(f"Node {self.node_id} has no peer with ID {node_id}")

    def get_prompt(self):
        tx_stack = getattr(self, "transaction_manager", None) and self.transaction_manager.tx_stack
        tx_str = f"[{tx_stack[-1]}]" if tx_stack else ""
        if self.is_leader:
            return f"shell-node{self.node_id}(LIDER){tx_str}> "
        return f"shell-node{self.node_id}{tx_str}> "


    def get_leader(self):
        if self.leader_id is None:
            return None

        if self.leader_id == self.node_id:
            return {"host": self.host, "port": self.port}

        return self.peer_manager.get_peer(self.leader_id)

    def crash(self):
        self.alive = False
        print(f"Node {self.node_id} is simulating a crash (unresponsive).")

    def revive(self):
        self.alive = True
        print(f"Node {self.node_id} is back online.")
