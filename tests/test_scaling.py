from basetest import *

from zfs_autobackup.ExecuteNode import ExecuteNode

run_orig=ExecuteNode.run
run_counter=0

def run_count(*args, **kwargs):
    global run_counter
    run_counter=run_counter+1
    return (run_orig(*args, **kwargs))

class TestZfsScaling(unittest2.TestCase):

    def setUp(self):
        prepare_zpools()
        self.longMessage = True

    def test_manysnapshots(self):
        """count the number of commands when there are many snapshots."""

        snapshot_count=100

        print("Creating many snapshots...")
        s=""
        for i in range(1970,1970+snapshot_count):
            s=s+"zfs snapshot test_source1/fs1@test-{:04}1111000000;".format(i)

        shelltest(s)

        global run_counter

        run_counter=0
        with patch.object(ExecuteNode,'run', run_count) as p:

            with mocktime("20101112000000"):
                self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose --keep-source=10000 --keep-target=10000 --no-holds --allow-empty".split(" ")).run())


            #this triggers if you make a change with an impact of more than O(snapshot_count/2)
            expected_runs=342
            print("EXPECTED RUNS: {}".format(expected_runs))
            print("ACTUAL RUNS  : {}".format(run_counter))
            self.assertLess(abs(run_counter-expected_runs), snapshot_count/2)


        run_counter=0
        with patch.object(ExecuteNode,'run', run_count) as p:

            with mocktime("20101112000001"):
                self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose --keep-source=10000 --keep-target=10000 --no-holds --allow-empty".split(" ")).run())


            #this triggers if you make a change with a performance impact of more than O(snapshot_count/2)
            expected_runs=47
            print("EXPECTED RUNS: {}".format(expected_runs))
            print("ACTUAL RUNS  : {}".format(run_counter))
            self.assertLess(abs(run_counter-expected_runs), snapshot_count/2)

    def test_manydatasets(self):
        """count the number of commands when when there are many datasets"""

        dataset_count=100

        print("Creating many datasets...")
        s=""
        for i in range(0,dataset_count):
            s=s+"zfs create test_source1/fs1/{};".format(i)

        shelltest(s)

        global run_counter

        #first run
        run_counter=0
        with patch.object(ExecuteNode,'run', run_count) as p:

            with mocktime("20101112000000"):
                self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose --no-holds --allow-empty".split(" ")).run())


            #this triggers if you make a change with an impact of more than O(snapshot_count/2)`
            expected_runs=842
            print("EXPECTED RUNS: {}".format(expected_runs))
            print("ACTUAL RUNS: {}".format(run_counter))
            self.assertLess(abs(run_counter-expected_runs), dataset_count/2)


        #second run, should have higher number of expected_runs
        run_counter=0
        with patch.object(ExecuteNode,'run', run_count) as p:

            with mocktime("20101112000001"):
                self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose --no-holds --allow-empty".split(" ")).run())


            #this triggers if you make a change with a performance impact of more than O(snapshot_count/2)
            expected_runs=1047
            print("EXPECTED RUNS: {}".format(expected_runs))
            print("ACTUAL RUNS: {}".format(run_counter))
            self.assertLess(abs(run_counter-expected_runs), dataset_count/2)
