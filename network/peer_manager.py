class PeerManager:
    """
    Keeps track of the other nodes known to this node:
    node_id -> {"host": ..., "port": ...}
    """

    def __init__(self, node):
        self.node = node
        self.peers = {}

    def add_peer(self, node_id, host, port):
        self.peers[node_id] = {"host": host, "port": port}

    def remove_peer(self, node_id):
        if node_id in self.peers:
            del self.peers[node_id]

    def get_peer(self, node_id):
        return self.peers.get(node_id)

    def all_peer_ids(self):
        return list(self.peers.keys())

    def peers_with_higher_id(self, node_id):
        return {
            pid: info for pid, info in list(self.peers.items())
            if pid > node_id
        }

    def __contains__(self, node_id):
        return node_id in self.peers

    def __len__(self):
        return len(self.peers)
