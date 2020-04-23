import unittest

import pfio
import chainerio


class TestAlias(unittest.TestCase):
    def test_alias(self):
        self.assertTrue(chainerio is pfio)
        self.assertEqual(list(chainerio.list("/")), list(pfio.list("/")))
