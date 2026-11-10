import unittest

from zfs_autobackup.Thinner import Thinner


class TestThinnerScheduleParsing(unittest.TestCase):
    def test_spaces_around_rules_are_ignored(self):
        thinner = Thinner("5, 1d1w")
        self.assertEqual(thinner.always_keep, 5)
        self.assertEqual(len(thinner.rules), 1)
        self.assertEqual(str(thinner.rules[0]), "1d1w")

    def test_trailing_comma_is_ignored(self):
        thinner = Thinner("5,")
        self.assertEqual(thinner.always_keep, 5)
        self.assertEqual(thinner.rules, [])

    def test_invalid_schedule_still_rejected(self):
        with self.assertRaises(Exception):
            Thinner("XXX")


if __name__ == "__main__":
    unittest.main()
