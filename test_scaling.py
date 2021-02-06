from basetest import *
import time
from bin.zfs_autobackup import *

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

    def test_manysnaps(self):
        """count the number of commands when there are many snapshots."""

        snapshot_count=100

        # create bunch of snapshots
        s=""
        for i in range(1970,1970+snapshot_count):
            s=s+"zfs snapshot test_source1/fs1@test-{:04}1111000000;".format(i)

        shelltest(s)

        global run_counter

        run_counter=0
        with patch.object(ExecuteNode,'run', run_count) as p:

            with patch('time.strftime', return_value="20101112000000"):
                self.assertFalse(ZfsAutobackup("test test_target1 --verbose --keep-source=10000 --keep-target=10000 --no-holds --allow-empty".split(" ")).run())


            #this triggers if you make a change with an impact of more than O(snapshot_count/2)
            expected_runs=343
            print("ACTUAL RUNS: {}".format(run_counter))
            self.assertLess(abs(run_counter-expected_runs), snapshot_count/2)


        run_counter=0
        with patch.object(ExecuteNode,'run', run_count) as p:

            with patch('time.strftime', return_value="20101112000001"):
                self.assertFalse(ZfsAutobackup("test test_target1 --verbose --keep-source=10000 --keep-target=10000 --no-holds --allow-empty".split(" ")).run())


            #this triggers if you make a change with an impact of more than O(snapshot_count/2)
            expected_runs=47
            print("ACTUAL RUNS: {}".format(run_counter))
            self.assertLess(abs(run_counter-expected_runs), snapshot_count/2)

