import unittest as _unittest
try:
    import unittest.mock as _mock
except ImportError:
    import mock as _mock

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
        return _bes.emit(
            sort_keys=True, connection_class=_mock.MagicMock(),
            *args, **kwargs)

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
