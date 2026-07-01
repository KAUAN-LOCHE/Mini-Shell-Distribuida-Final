class HoldbackQueue:
    """
    Holds multicast messages that arrived out of order, indexed by
    sender -> {sequence: payload}, until they can be delivered in order.
    """

    def __init__(self):
        self._queue = {}

    def add(self, sender, sequence, payload):
        self._queue.setdefault(sender, {})[sequence] = payload

    def has(self, sender, sequence):
        return sequence in self._queue.get(sender, {})

    def pop(self, sender, sequence):
        return self._queue[sender].pop(sequence)

    def pending_for(self, sender):
        return sorted(self._queue.get(sender, {}).keys())
