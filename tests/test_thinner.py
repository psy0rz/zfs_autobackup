from basetest import *
import pprint

from zfs_autobackup.Thinner import Thinner

# randint is different in python 2 vs 3
randint_compat = lambda lo, hi: lo + int(random.random() * (hi + 1 - lo))


class Thing:
    def __init__(self, timestamp):
        self.timestamp=timestamp

    def __str__(self):
        # age=now-self.timestamp
        struct=time.gmtime(self.timestamp)
        return("{}".format(time.strftime("%Y-%m-%d %H:%M:%S",struct)))


class TestThinner(unittest2.TestCase):

    # def setUp(self):

        # return super().setUp()

    def test_exceptions(self):
        with self.assertRaisesRegexp(Exception, "^Invalid period"):
            ThinnerRule("12X12m")

        with self.assertRaisesRegexp(Exception, "^Invalid ttl"):
            ThinnerRule("12d12X")

        with self.assertRaisesRegexp(Exception, "^Period cant be"):
            ThinnerRule("12d1d")

        with self.assertRaisesRegexp(Exception, "^Invalid schedule"):
            ThinnerRule("XXX")

        with self.assertRaisesRegexp(Exception, "^Number of"):
            Thinner("-1")


    def test_incremental(self):
        ok=['2023-01-03 10:53:16',
            '2024-01-02 15:43:29',
            '2025-01-01 06:15:32',
            '2026-01-01 02:48:23',
            '2026-04-07 20:07:36',
            '2026-05-07 02:30:29',
            '2026-06-06 01:19:46',
            '2026-07-06 06:38:09',
            '2026-08-05 05:08:53',
            '2026-09-04 03:33:04',
            '2026-10-04 05:27:09',
            '2026-11-04 04:01:17',
            '2026-12-03 13:49:56',
            '2027-01-01 17:02:00',
            '2027-01-03 04:26:42',
            '2027-02-01 14:16:02',
            '2027-02-12 03:31:02',
            '2027-02-18 00:33:10',
            '2027-02-26 21:09:54',
            '2027-03-02 08:05:18',
            '2027-03-03 16:46:09',
            '2027-03-04 06:39:14',
            '2027-03-06 03:35:41',
            '2027-03-08 12:24:42',
            '2027-03-08 20:34:57']




        #some arbitrary date
        now=1589229252
        #we want deterministic results
        random.seed(1337)
        thinner=Thinner("5,10s1min,1d1w,1w1m,1m12m,1y5y")
        things=[]

        #thin incrementally while adding
        for i in range(0,5000):

            #increase random amount of time and maybe add a thing
            now=now+randint_compat(0,3600*24)
            if random.random()>=0.5:
                things.append(Thing(now))

            (keeps, removes)=thinner.thin(things, keep_objects=[], now=now)
            things=keeps


        result=[]
        for thing in things:
            result.append(str(thing))

        print("Thinner result incremental:")
        pprint.pprint(result)

        self.assertEqual(result, ok)


    def test_full(self):
        ok=['2022-03-09 01:56:23',
            '2023-01-03 10:53:16',
            '2024-01-02 15:43:29',
            '2025-01-01 06:15:32',
            '2026-01-01 02:48:23',
            '2026-03-14 09:08:04',
            '2026-04-07 20:07:36',
            '2026-05-07 02:30:29',
            '2026-06-06 01:19:46',
            '2026-07-06 06:38:09',
            '2026-08-05 05:08:53',
            '2026-09-04 03:33:04',
            '2026-10-04 05:27:09',
            '2026-11-04 04:01:17',
            '2026-12-03 13:49:56',
            '2027-01-01 17:02:00',
            '2027-01-03 04:26:42',
            '2027-02-01 14:16:02',
            '2027-02-08 02:41:14',
            '2027-02-12 03:31:02',
            '2027-02-18 00:33:10',
            '2027-02-26 21:09:54',
            '2027-03-02 08:05:18',
            '2027-03-03 16:46:09',
            '2027-03-04 06:39:14',
            '2027-03-06 03:35:41',
            '2027-03-08 12:24:42',
            '2027-03-08 20:34:57']

        #some arbitrary date
        now=1589229252
        #we want deterministic results
        random.seed(1337)
        thinner=Thinner("5,10s1min,1d1w,1w1m,1m12m,1y5y")
        things=[]

        for i in range(0,5000):

            #increase random amount of time and maybe add a thing
            now=now+randint_compat(0,3600*24)
            if random.random()>=0.5:
                things.append(Thing(now))

        (things, removes)=thinner.thin(things, keep_objects=[], now=now)

        result=[]
        for thing in things:
            result.append(str(thing))

        print("Thinner result full:")
        pprint.pprint(result)

        self.assertEqual(result, ok)


# if __name__ == '__main__':
#     unittest.main()
