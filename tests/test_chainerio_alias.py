import unittest

import chainerio
import pfio


class TestAlias(unittest.TestCase):
    def test_alias(self):
        self.assertTrue(chainerio is pfio)
        self.assertEqual(list(chainerio.list("/")), list(pfio.list("/")))
