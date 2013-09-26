import socket as _socket
import threading as _threading


class UDPListener (object):
    def __init__(self, host='', port=9000, buffsize=65565, count=0):
        self.host = host
        self.port = port
        self.bufsize = buffsize
        self.count = count
        self._sock = None
        self._thread = None
        self.messages = []

    def __enter__(self):
        self._sock = _socket.socket(_socket.AF_INET, _socket.SOCK_DGRAM)
        self._sock.bind((self.host, self.port))
        self._thread = _threading.Thread(
            name='UDP server',
            target=self._run)
        self._thread.start()
        return self

    def __exit__(self, *exc_info):
        if self._thread is not None:
            try:
                self.status = self._thread.join()
            finally:
                self._thread = None

    def _run(self):
        try:
            self._listen()
        finally:
            if self._sock is not None:
                try:
                    self._sock.close()
                finally:
                    self._sock = None

    def _listen(self):
        for i in range(self.count):
            self.messages.append(self._sock.recvfrom(self.bufsize))
