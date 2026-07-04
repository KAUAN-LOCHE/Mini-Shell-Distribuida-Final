import socket
import threading

from network.message import Message
from distributed.message_types import MessageType


class Server(threading.Thread):

    BUFFER_SIZE = 4096

    def __init__(self, host, port, node):

        super().__init__(daemon=True)

        self.host = host
        self.port = port
        self.node = node

        self._socket = None

    def run(self):

        self._socket = socket.socket(
            socket.AF_INET,
            socket.SOCK_STREAM
        )

        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        self._socket.bind((self.host, self.port))

        self._socket.listen()

        print(f"Listening on {self.host}:{self.port}")

        while True:

            conn, addr = self._socket.accept()

            threading.Thread(
                target=self.handle_client,
                args=(conn, addr),
                daemon=True
            ).start()

    def handle_client(self, conn, addr):

        try:

            data = conn.recv(self.BUFFER_SIZE)

            if not data:
                return

            # Simulated crash: a "dead" node drops every incoming
            # connection without answering, so peers will detect it as
            # unreachable (used to trigger the Bully algorithm).
            if not self.node.alive:
                conn.close()
                return

            message = Message.from_json(data.decode())

            response = self._dispatch(message)

            if response is not None:
                conn.sendall(response.to_json().encode())

        except Exception as e:

            try:
                error = Message(
                    MessageType.RESPONSE,
                    sender=self.node.node_id,
                    payload=str(e)
                )
                conn.sendall(error.to_json().encode())
            except Exception:
                pass

        finally:
            conn.close()

    def _dispatch(self, message):

        node = self.node

        ###################################################

        if message.type == MessageType.COMMAND.value:

            result = node.parser.execute(message.payload)

            return Message(
                MessageType.RESPONSE,
                sender=node.node_id,
                payload=result
            )

        ###################################################

        elif message.type == MessageType.LOCK.value:

            return node.lock_manager.handle_lock(message)

        ###################################################

        elif message.type == MessageType.UNLOCK.value:

            return node.lock_manager.handle_unlock(message)

        ###################################################

        elif message.type == MessageType.RICART_REQUEST.value:

            return node.mutex_manager.handle_ricart_request(message)

        ###################################################

        elif message.type == MessageType.RICART_REPLY.value:

            return node.mutex_manager.handle_ricart_reply(message)

        ###################################################

        elif message.type == MessageType.MAEKAWA_REQUEST.value:

            return node.mutex_manager.handle_maekawa_request(message)

        ###################################################

        elif message.type == MessageType.MAEKAWA_VOTE.value:

            return node.mutex_manager.handle_maekawa_vote(message)

        ###################################################

        elif message.type == MessageType.MAEKAWA_RELEASE.value:

            return node.mutex_manager.handle_maekawa_release(message)


        ###################################################

        elif message.type == MessageType.ELECTION.value:

            return node.leader_manager.handle_election(message)

        ###################################################

        elif message.type == MessageType.COORDINATOR.value:

            return node.leader_manager.handle_coordinator(message)

        ###################################################

        elif message.type == MessageType.RING_ELECTION.value:

            return node.leader_manager.handle_ring_election(message)

        ###################################################

        elif message.type == MessageType.RING_COORDINATOR.value:

            return node.leader_manager.handle_ring_coordinator(message)


        ###################################################

        elif message.type == MessageType.TOTAL_SEQUENCER_REQUEST.value:

            return node.multicast_manager.handle_sequencer_request(message)

        ###################################################

        elif message.type == MessageType.CONSENSUS_PROPOSE.value:

            return node.consensus_manager.handle_proposal(message)

        ###################################################

        elif message.type == MessageType.CONSENSUS_DECIDE.value:

            return node.consensus_manager.handle_decision(message)

        ###################################################


        elif message.type == MessageType.TX_PREPARE.value:

            return node.transaction_manager.handle_prepare(message)

        ###################################################

        elif message.type == MessageType.TX_GLOBAL_COMMIT.value:

            return node.transaction_manager.handle_global_commit(message)

        ###################################################

        elif message.type == MessageType.TX_GLOBAL_ABORT.value:

            return node.transaction_manager.handle_global_abort(message)

        ###################################################

        elif message.type == MessageType.MULTICAST.value:


            return node.multicast_manager.handle_message(message)


        ###################################################

        elif message.type == MessageType.PEER_JOIN.value:

            payload = message.payload or {}

            node.add_peer(
                message.sender,
                payload.get("host"),
                payload.get("port")
            )

            return Message(
                MessageType.RESPONSE,
                sender=node.node_id,
                payload="PEER_ADDED"
            )

        ###################################################

        elif message.type == MessageType.RESPONSE.value:

            # Generic health-check / ping used by the leader-monitoring
            # heartbeat: any RESPONSE just gets echoed back as PONG.
            return Message(
                MessageType.RESPONSE,
                sender=node.node_id,
                payload="PONG"
            )

        ###################################################

        else:

            return Message(
                MessageType.RESPONSE,
                sender=node.node_id,
                payload="Unknown message."
            )
