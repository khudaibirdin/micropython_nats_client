from . import connection


class Publisher(connection.Connection):
    def __init__(self, host="localhost", port=4222):
        super().__init__(host, port)
    
    def send(self, topic: str, msg: str):
        """Отправить сообщение в топик"""
        self._send_message(topic, self._prepare_message(msg))

    def _prepare_message(self, msg: str) -> bytes:
        return msg.encode('utf-8')
        
    def _get_pub_cmd(self, topic: str, parcel: bytes) -> bytes:
        pub_cmd = f'PUB {topic} {len(parcel)}\r\n'.encode('utf-8')
        return pub_cmd
    
    def _send_message(self, topic: str, parcel: bytes):
        self.send_msg(self._get_pub_cmd(topic, parcel), parcel + b'\r\n')
        