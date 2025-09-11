import usocket as socket


def with_connection(func):
    """Connecting/disconnecting to NATS"""

    def wrapper(self, *args, **kwargs):
        try:
            self._connect()
            print("[Connection][info] Socket connected")
        except Exception as error:
            print(f"[Connection][error] Socket conncection error: {error}")
            return

        try:
            self._send_connection_info()
            print("[Connection][info] Sent NATS CONNECT command")
        except Exception as error:
            print(f"[Connection][error] NATS CONNECT command error: {error}")
            self._disconnect()
            return

        try:
            return func(self, *args, **kwargs)
        finally:
            self._disconnect()

    return wrapper


class Connection:
    def __init__(self, host: str = "localhost", port: int = 7432):
        self.host = host
        self.port = port

    @with_connection
    def send_msg(self, command: bytes, msg: bytes):
        """Send socket messages acc to NATS protocol"""
        try:
            self._send_msg(command)
            print("[Connection][info] Sent NATS PUB command")
        except Exception as error:
            print(f"[Connection][error] NATS PUB command error: {error}")
            return

        try:
            self._send_msg(msg)
            print("[Connection][info] Sent NATS payload")
        except Exception as error:
            print(f"[Connection][error] NATS payload send error: {error}")
            return

    def _connect(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.settimeout(5)
        self.socket.connect((self.host, self.port))

    def _disconnect(self):
        if self.socket:
            self.socket.close()

    def _send_connection_info(self):
        connect_cmd = b'CONNECT {"verbose":false,"pedantic":false}\r\n'
        self._send_msg(connect_cmd)

    def _send_msg(self, msg: bytes) -> int:
        sent_bytes = self.socket.send(msg)
        if not sent_bytes or sent_bytes == 0:
            raise MessageSendError()


class MessageSendError(Exception):
    message = "Socket send receive 0"

    def __init__(self):
        super().__init__(self.message)
