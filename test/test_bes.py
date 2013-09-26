import unittest as _unittest

import bes as _bes
from . import udp_listener as _udp_listener


class ConnectionTestCase (_unittest.TestCase):
    def test_udp_connection(self):
        with _udp_listener.UDPListener(count=2) as listener:
            with _bes.Connection(
                    host=listener.host, port=listener.port, protocol='UDP'
                    ) as connection:
                connection.send(b'Hello!')
                connection.send(b'Goodbye!')
        self.assertEqual(
            [msg for msg,addr in listener.messages],
            [b'Hello!', b'Goodbye!'])


class EmitTestCase (_unittest.TestCase):
    def _call_emit(self, *args, **kwargs):
        with _udp_listener.UDPListener(count=1) as listener:
            _bes.emit(
                host=listener.host, port=listener.port, protocol='UDP',
                sort_keys=True, *args, **kwargs)
        self.assertEqual(len(listener.messages), 1)
        self.assertEqual(len(listener.messages), 1)
        return listener.messages[0][0]

    def test_emit(self):
        message = self._call_emit(
            payload={'hello': 'world', 'goodbye': 'everybody'})
        self.assertEqual(
            message,
            b'\n'.join([
                b'{"index": {"_index": "log", "_type": "record"}}',
                b'{"goodbye": "everybody", "hello": "world"}',
                b'',
            ]))
