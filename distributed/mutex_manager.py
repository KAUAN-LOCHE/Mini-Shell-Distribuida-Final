import threading
import math
from network.message import Message
from network.client import Client
from distributed.message_types import MessageType


class MutexState:
    def __init__(self):
        self.state = "RELEASED"  # RELEASED, WANTED, HELD
        self.timestamp = 0
        self.replies_received = set()
        self.deferred_requests = []  # list of (sender_id, timestamp)
        self.condition = threading.Condition()

        # Maekawa specific
        self.votes_received = set()


class MutexManager:
    """
    Implements distributed mutual exclusion using:
    - Ricart & Agrawala (Lamport Clocks)
    - Maekawa (Quorums/Voting sets)
    - Centralized (default fallback)
    """

    def __init__(self, node):
        self.node = node
        self._lock = threading.Lock()
        self.resources = {}  # resource_name -> MutexState

        # Maekawa voting state (as a voter/participant)
        self.voted_for = {}  # resource -> node_id or None
        self.vote_queue = {}  # resource -> list of sender_ids

    def _get_or_create_resource(self, resource) -> MutexState:
        with self._lock:
            if resource not in self.resources:
                self.resources[resource] = MutexState()
            return self.resources[resource]

    # ------------------------------------------------------------------
    # Ricart & Agrawala Implementation
    # ------------------------------------------------------------------

    def acquire_ricart(self, resource):
        node = self.node
        res_state = self._get_or_create_resource(resource)

        with res_state.condition:
            res_state.state = "WANTED"
            # Get timestamp
            res_state.timestamp = node.increment_lamport_clock()
            res_state.replies_received = set()

            peers = list(node.peer_manager.peers.keys())
            if not peers:
                res_state.state = "HELD"
                return True

            print(f"Node {node.node_id}: requesting RA lock for '{resource}' at ts={res_state.timestamp}")

            message = Message(
                MessageType.RICART_REQUEST,
                sender=node.node_id,
                payload={"resource": resource, "timestamp": res_state.timestamp}
            )

            expected_replies = set()
            for pid, info in list(node.peer_manager.peers.items()):
                expected_replies.add(pid)
                # Send request asynchronously to avoid deadlock
                threading.Thread(target=Client.send_to, args=(info["host"], info["port"], message), kwargs={"timeout": 2.0}, daemon=True).start()

            # Wait for replies
            def all_replies_in():
                # Only expect replies from alive peers
                alive_peers = {pid for pid in expected_replies if pid in node.peer_manager.peers}
                return alive_peers.issubset(res_state.replies_received)

            success = res_state.condition.wait_for(all_replies_in, timeout=5.0)

            if success:
                res_state.state = "HELD"
                print(f"Node {node.node_id}: acquired RA lock for '{resource}'")
                return True
            else:
                print(f"Node {node.node_id}: failed to acquire RA lock for '{resource}' (timeout)")
                res_state.state = "RELEASED"
                return False

    def release_ricart(self, resource):
        node = self.node
        res_state = self._get_or_create_resource(resource)

        with res_state.condition:
            res_state.state = "RELEASED"
            deferred = list(res_state.deferred_requests)
            res_state.deferred_requests.clear()
            res_state.condition.notify_all()

        print(f"Node {node.node_id}: releasing RA lock for '{resource}', sending {len(deferred)} deferred replies.")

        reply_msg = Message(
            MessageType.RICART_REPLY,
            sender=node.node_id,
            payload={"resource": resource}
        )

        for pid in deferred:
            info = node.peer_manager.get_peer(pid)
            if info:
                threading.Thread(target=Client.send_to, args=(info["host"], info["port"], reply_msg), daemon=True).start()

    def handle_ricart_request(self, message: Message):
        node = self.node
        sender_id = message.sender
        resource = message.payload["resource"]
        timestamp = message.payload["timestamp"]

        node.increment_lamport_clock(timestamp)
        res_state = self._get_or_create_resource(resource)

        defer = False
        with res_state.condition:
            if res_state.state == "HELD" and res_state.requested_resource == resource:
                defer = True
            elif res_state.state == "WANTED" and (
                res_state.timestamp < timestamp or 
                (res_state.timestamp == timestamp and node.node_id < sender_id)
            ):
                defer = True

            if defer:
                res_state.deferred_requests.append(sender_id)
                print(f"Node {node.node_id}: deferring RA reply to {sender_id} for '{resource}'")
                return None

        # Reply immediately
        return Message(
            MessageType.RICART_REPLY,
            sender=node.node_id,
            payload={"resource": resource}
        )

    def handle_ricart_reply(self, message: Message):
        resource = message.payload["resource"]
        res_state = self._get_or_create_resource(resource)

        with res_state.condition:
            res_state.replies_received.add(message.sender)
            res_state.condition.notify_all()

        return Message(MessageType.RESPONSE, sender=self.node.node_id, payload="ACK")

    # ------------------------------------------------------------------
    # Maekawa Implementation
    # ------------------------------------------------------------------

    def build_quorum(self):
        node = self.node
        all_nodes = sorted([node.node_id] + list(node.peer_manager.peers.keys()))
        n = len(all_nodes)
        if n <= 2:
            return set(all_nodes)

        r = int(math.sqrt(n))
        c = math.ceil(n / r)
        grid = []
        for idx in range(r):
            grid.append(all_nodes[idx*c : (idx+1)*c])

        my_row, my_col = -1, -1
        for row_idx, row in enumerate(grid):
            if node.node_id in row:
                my_row = row_idx
                my_col = row.index(node.node_id)
                break

        if my_row == -1:
            return set(all_nodes)

        quorum = set()
        quorum.update(grid[my_row])
        for row in grid:
            if my_col < len(row):
                quorum.add(row[my_col])
        return quorum

    def acquire_maekawa(self, resource):
        node = self.node
        res_state = self._get_or_create_resource(resource)

        with res_state.condition:
            res_state.state = "WANTED"
            res_state.votes_received = set()

            quorum = self.build_quorum()
            print(f"Node {node.node_id}: requesting Maekawa lock for '{resource}' to quorum {list(quorum)}")

            message = Message(
                MessageType.MAEKAWA_REQUEST,
                sender=node.node_id,
                payload={"resource": resource}
            )

            expected_votes = set(quorum)

            # Send requests
            for pid in quorum:
                if pid == node.node_id:
                    # Vote local
                    vote = self.handle_maekawa_request(message)
                    if vote and vote.type == MessageType.MAEKAWA_VOTE.value:
                        res_state.votes_received.add(node.node_id)
                else:
                    info = node.peer_manager.get_peer(pid)
                    if info:
                        threading.Thread(target=Client.send_to, args=(info["host"], info["port"], message), kwargs={"timeout": 2.0}, daemon=True).start()

            def all_votes_in():
                # Only expect votes from alive members in the quorum
                alive_quorum = {pid for pid in expected_votes if pid == node.node_id or pid in node.peer_manager.peers}
                return alive_quorum.issubset(res_state.votes_received)

            success = res_state.condition.wait_for(all_votes_in, timeout=5.0)

            if success:
                res_state.state = "HELD"
                print(f"Node {node.node_id}: acquired Maekawa lock for '{resource}'")
                return True
            else:
                print(f"Node {node.node_id}: failed to acquire Maekawa lock for '{resource}' (timeout)")
                res_state.state = "RELEASED"
                # Release votes
                self.release_maekawa(resource)
                return False

    def release_maekawa(self, resource):
        node = self.node
        res_state = self._get_or_create_resource(resource)

        with res_state.condition:
            res_state.state = "RELEASED"
            res_state.condition.notify_all()

        quorum = self.build_quorum()
        release_msg = Message(
            MessageType.MAEKAWA_RELEASE,
            sender=node.node_id,
            payload={"resource": resource}
        )

        for pid in quorum:
            if pid == node.node_id:
                self.handle_maekawa_release(release_msg)
            else:
                info = node.peer_manager.get_peer(pid)
                if info:
                    threading.Thread(target=Client.send_to, args=(info["host"], info["port"], release_msg), daemon=True).start()

    def handle_maekawa_request(self, message: Message):
        sender_id = message.sender
        resource = message.payload["resource"]

        with self._lock:
            current_vote = self.voted_for.get(resource)
            if current_vote is None:
                self.voted_for[resource] = sender_id
                print(f"Node {self.node.node_id}: voting for {sender_id} on '{resource}'")
                return Message(
                    MessageType.MAEKAWA_VOTE,
                    sender=self.node.node_id,
                    payload={"resource": resource}
                )
            else:
                if resource not in self.vote_queue:
                    self.vote_queue[resource] = []
                if sender_id not in self.vote_queue[resource]:
                    self.vote_queue[resource].append(sender_id)
                print(f"Node {self.node.node_id}: queueing Maekawa request from {sender_id} for '{resource}' (voted for {current_vote})")
                return None

    def handle_maekawa_vote(self, message: Message):
        resource = message.payload["resource"]
        res_state = self._get_or_create_resource(resource)

        with res_state.condition:
            res_state.votes_received.add(message.sender)
            res_state.condition.notify_all()

        return Message(MessageType.RESPONSE, sender=self.node.node_id, payload="ACK")

    def handle_maekawa_release(self, message: Message):
        sender_id = message.sender
        resource = message.payload["resource"]

        with self._lock:
            current_vote = self.voted_for.get(resource)
            if current_vote == sender_id:
                queue = self.vote_queue.get(resource, [])
                if queue:
                    next_req = queue.pop(0)
                    self.voted_for[resource] = next_req
                    print(f"Node {self.node.node_id}: releasing vote from {sender_id} for '{resource}', voting for next: {next_req}")
                    vote_msg = Message(
                        MessageType.MAEKAWA_VOTE,
                        sender=self.node.node_id,
                        payload={"resource": resource}
                    )
                    if next_req == self.node.node_id:
                        self.handle_maekawa_vote(vote_msg)
                    else:
                        info = self.node.peer_manager.get_peer(next_req)
                        if info:
                            threading.Thread(target=Client.send_to, args=(info["host"], info["port"], vote_msg), daemon=True).start()
                else:
                    self.voted_for[resource] = None
                    print(f"Node {self.node.node_id}: released vote for '{resource}'")

        return Message(MessageType.RESPONSE, sender=self.node.node_id, payload="ACK")
