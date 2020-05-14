from basetest import *


class Thing:
    def __init__(self, timestamp):
        self.timestamp=timestamp

    def __str__(self):
        # age=now-self.timestamp
        struct=time.localtime(self.timestamp)
        return("{}".format(time.strftime("%Y-%m-%d %H:%M:%S",struct)))


class TestThinner(unittest.TestCase):

    def setUp(self):

        return super().setUp()

    def test_incremental(self):

        ok=['2023-01-01 11:09:50',
        '2024-01-01 21:06:35',
        '2025-01-01 10:59:44',
        '2026-01-01 19:06:41',
        '2026-03-08 03:27:07',
        '2026-04-07 04:29:04',
        '2026-05-07 20:39:31',
        '2026-06-06 08:06:14',
        '2026-07-06 05:53:12',
        '2026-08-05 08:23:43',
        '2026-09-04 23:13:46',
        '2026-10-04 02:50:48',
        '2026-11-03 02:52:55',
        '2026-12-03 16:04:25',
        '2027-01-01 10:02:16',
        '2027-01-02 10:59:16',
        '2027-01-28 10:54:49',
        '2027-02-01 09:59:47',
        '2027-02-04 04:24:33',
        '2027-02-11 02:51:49',
        '2027-02-18 05:09:25',
        '2027-02-19 15:21:39',
        '2027-02-20 14:41:38',
        '2027-02-21 08:33:50',
        '2027-02-22 08:39:18',
        '2027-02-23 08:52:18',
        '2027-02-24 03:16:31',
        '2027-02-24 03:17:08',
        '2027-02-24 06:26:13',
        '2027-02-24 13:56:41']

        #some arbitrary date
        now=1589229252
        #we want deterministic results
        random.seed(1337)
        thinner=Thinner("5,10s1min,1d1w,1w1m,1m12m,1y5y")
        things=[]

        #thin incrementally while adding
        for i in range(0,5000):

            #increase random amount of time and maybe add a thing
            now=now+random.randint(0,3600*24)
            if random.random()>=0:
                things.append(Thing(now))

            (keeps, removes)=thinner.thin(things, now=now)
            things=keeps


        result=[]
        for thing in things:
            result.append(str(thing))
        
        print("Thinner result:")
        pprint.pprint(result)

        self.assertEqual(result, ok)


    def test_full(self):

        ok=['2022-02-24 16:54:37',
            '2023-01-01 11:09:50',
            '2024-01-01 21:06:35',
            '2025-01-01 10:59:44',
            '2026-01-01 19:06:41',
            '2026-03-02 00:23:58',
            '2026-03-08 03:27:07',
            '2026-04-07 04:29:04',
            '2026-05-07 20:39:31',
            '2026-06-06 08:06:14',
            '2026-07-06 05:53:12',
            '2026-08-05 08:23:43',
            '2026-09-04 23:13:46',
            '2026-10-04 02:50:48',
            '2026-11-03 02:52:55',
            '2026-12-03 16:04:25',
            '2027-01-01 10:02:16',
            '2027-01-02 10:59:16',
            '2027-01-25 21:00:35',
            '2027-01-28 10:54:49',
            '2027-02-01 09:59:47',
            '2027-02-04 04:24:33',
            '2027-02-11 02:51:49',
            '2027-02-18 05:09:25',
            '2027-02-19 15:21:39',
            '2027-02-20 14:41:38',
            '2027-02-21 08:33:50',
            '2027-02-22 08:39:18',
            '2027-02-23 08:52:18',
            '2027-02-24 03:16:31',
            '2027-02-24 03:17:08',
            '2027-02-24 06:26:13',
            '2027-02-24 13:56:41']


        #some arbitrary date
        now=1589229252
        #we want deterministic results
        random.seed(1337)
        thinner=Thinner("5,10s1min,1d1w,1w1m,1m12m,1y5y")
        things=[]

        for i in range(0,5000):

            #increase random amount of time and maybe add a thing
            now=now+random.randint(0,3600*24)
            if random.random()>=0:
                things.append(Thing(now))

        (things, removes)=thinner.thin(things, now=now)

        result=[]
        for thing in things:
            result.append(str(thing))
        
        print("Thinner result:")
        pprint.pprint(result)

        self.assertEqual(result, ok)


if __name__ == '__main__':
    unittest.main()