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
