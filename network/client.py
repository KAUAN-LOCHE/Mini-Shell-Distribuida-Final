import socket

from network.message import Message


class Client:
    """
    A lightweight TCP client used to talk to a single peer (host, port).
    Instances are meant to be short-lived: one connection per request,
    which avoids shared-state issues when multiple threads need to talk
    to different nodes (leader, peers, etc.) at the same time.
    """

    BUFFER_SIZE = 4096
    DEFAULT_TIMEOUT = 3.0

    def __init__(self, host: str, port: int, timeout: float = DEFAULT_TIMEOUT):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.socket = None
        self.connected = False

    def connect(self) -> bool:
        """
        Establishes a connection to the peer.
        """

        if self.connected:
            return True

        try:

            self.socket = socket.socket(
                socket.AF_INET,
                socket.SOCK_STREAM
            )

            self.socket.settimeout(self.timeout)

            self.socket.connect((self.host, self.port))

            self.connected = True

            return True

        except Exception as e:

            print(f"Error connecting to {self.host}:{self.port} -> {e}")

            return False

    def send(self, data: str) -> bool:
        """
        Sends a raw string to the peer.
        """

        if not self.connected:
            return False

        try:

            self.socket.sendall(data.encode())

            return True

        except Exception as e:

            print(f"Error sending data: {e}")

            self.connected = False

            return False

    def send_message(self, message: Message) -> bool:
        """
        Sends a Message object.
        """

        return self.send(message.to_json())

    def receive(self) -> "Message | None":
        """
        Receives a Message object from the peer.
        """

        if not self.connected:
            return None

        try:

            response = self.socket.recv(self.BUFFER_SIZE)

            if not response:
                self.connected = False
                return None

            return Message.from_json(response.decode())

        except Exception as e:

            print(f"Error receiving message: {e}")

            self.connected = False

            return None

    def execute(self, message: Message) -> "Message | None":
        """
        Sends a Message object and waits for the response.
        """

        if not self.connected:
            if not self.connect():
                return None

        if not self.send_message(message):
            return None

        return self.receive()

    def disconnect(self):
        """
        Closes the connection to the peer.
        """

        if self.connected:

            try:
                self.socket.close()
            except Exception as e:
                print(f"Error disconnecting: {e}")
            finally:
                self.connected = False
                self.socket = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.disconnect()

    @staticmethod
    def send_to(host, port, message: Message, timeout: float = DEFAULT_TIMEOUT) -> "Message | None":
        """
        Convenience helper: opens a short-lived connection to (host, port),
        sends `message`, waits for a response and closes the connection.
        Returns None (and prints a warning) if the peer is unreachable.
        """

        try:
            with Client(host, port, timeout=timeout) as client:
                if not client.connected:
                    return None
                return client.execute(message)
        except Exception as e:
            print(f"Error communicating with {host}:{port} -> {e}")
            return None
