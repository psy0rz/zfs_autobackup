from zfs_autobackup.CmdPipe import CmdPipe

from basetest import *
import time


class TestZfsAutobackup(unittest2.TestCase):

    def setUp(self):
        prepare_zpools()
        self.longMessage=True

    def test_invalidpars(self):

        self.assertEqual(ZfsAutobackup("test test_target1 --no-progress --keep-source -1".split(" ")).run(), 255)

        with OutputIO() as buf:
            with redirect_stdout(buf):
                self.assertEqual(ZfsAutobackup("test test_target1 --no-progress --resume --verbose --no-snapshot".split(" ")).run(), 0)

            print(buf.getvalue())
            self.assertIn("The --resume", buf.getvalue())

        with OutputIO() as buf:
            with redirect_stderr(buf):
                self.assertEqual(ZfsAutobackup("test test_target_nonexisting --no-progress".split(" ")).run(), 255)

            print(buf.getvalue())
            # correct message?
            self.assertIn("Please create this dataset", buf.getvalue())


    def  test_snapshotmode(self):
        """test snapshot tool mode"""

        with patch('time.strftime', return_value="test-20101111000000"):
            self.assertFalse(ZfsAutobackup("test --no-progress --verbose".split(" ")).run())

        r=shelltest("zfs list -H -o name -r -t all "+TEST_POOLS)
        self.assertMultiLineEqual(r,"""
test_source1
test_source1/fs1
test_source1/fs1@test-20101111000000
test_source1/fs1/sub
test_source1/fs1/sub@test-20101111000000
test_source2
test_source2/fs2
test_source2/fs2/sub
test_source2/fs2/sub@test-20101111000000
test_source2/fs3
test_source2/fs3/sub
test_target1
""")

    def  test_defaults(self):

        with self.subTest("no datasets selected"):
            with OutputIO() as buf:
                with redirect_stderr(buf):
                    with patch('time.strftime', return_value="test-20101111000000"):
                        self.assertTrue(ZfsAutobackup("nonexisting test_target1 --verbose --debug --no-progress".split(" ")).run())

                print(buf.getvalue())
                #correct message?
                self.assertIn("No source filesystems selected", buf.getvalue())


        with self.subTest("defaults with full verbose and debug"):

            with patch('time.strftime', return_value="test-20101111000000"):
                self.assertFalse(ZfsAutobackup("test test_target1 --verbose --debug --no-progress".split(" ")).run())

            r=shelltest("zfs list -H -o name -r -t all "+TEST_POOLS)
            self.assertMultiLineEqual(r,"""
test_source1
test_source1/fs1
test_source1/fs1@test-20101111000000
test_source1/fs1/sub
test_source1/fs1/sub@test-20101111000000
test_source2
test_source2/fs2
test_source2/fs2/sub
test_source2/fs2/sub@test-20101111000000
test_source2/fs3
test_source2/fs3/sub
test_target1
test_target1/test_source1
test_target1/test_source1/fs1
test_target1/test_source1/fs1@test-20101111000000
test_target1/test_source1/fs1/sub
test_target1/test_source1/fs1/sub@test-20101111000000
test_target1/test_source2
test_target1/test_source2/fs2
test_target1/test_source2/fs2/sub
test_target1/test_source2/fs2/sub@test-20101111000000
""")

        with self.subTest("bare defaults, allow empty"):
            with patch('time.strftime', return_value="test-20101111000001"):
                self.assertFalse(ZfsAutobackup("test test_target1 --allow-empty --no-progress".split(" ")).run())


            r=shelltest("zfs list -H -o name -r -t all "+TEST_POOLS)
            self.assertMultiLineEqual(r,"""
test_source1
test_source1/fs1
test_source1/fs1@test-20101111000000
test_source1/fs1@test-20101111000001
test_source1/fs1/sub
test_source1/fs1/sub@test-20101111000000
test_source1/fs1/sub@test-20101111000001
test_source2
test_source2/fs2
test_source2/fs2/sub
test_source2/fs2/sub@test-20101111000000
test_source2/fs2/sub@test-20101111000001
test_source2/fs3
test_source2/fs3/sub
test_target1
test_target1/test_source1
test_target1/test_source1/fs1
test_target1/test_source1/fs1@test-20101111000000
test_target1/test_source1/fs1@test-20101111000001
test_target1/test_source1/fs1/sub
test_target1/test_source1/fs1/sub@test-20101111000000
test_target1/test_source1/fs1/sub@test-20101111000001
test_target1/test_source2
test_target1/test_source2/fs2
test_target1/test_source2/fs2/sub
test_target1/test_source2/fs2/sub@test-20101111000000
test_target1/test_source2/fs2/sub@test-20101111000001
""")

        with self.subTest("verify holds"):

            r=shelltest("zfs get -r userrefs test_source1 test_source2 test_target1")
            self.assertMultiLineEqual(r,"""
NAME                                                   PROPERTY  VALUE     SOURCE
test_source1                                           userrefs  -         -
test_source1/fs1                                       userrefs  -         -
test_source1/fs1@test-20101111000000                   userrefs  0         -
test_source1/fs1@test-20101111000001                   userrefs  1         -
test_source1/fs1/sub                                   userrefs  -         -
test_source1/fs1/sub@test-20101111000000               userrefs  0         -
test_source1/fs1/sub@test-20101111000001               userrefs  1         -
test_source2                                           userrefs  -         -
test_source2/fs2                                       userrefs  -         -
test_source2/fs2/sub                                   userrefs  -         -
test_source2/fs2/sub@test-20101111000000               userrefs  0         -
test_source2/fs2/sub@test-20101111000001               userrefs  1         -
test_source2/fs3                                       userrefs  -         -
test_source2/fs3/sub                                   userrefs  -         -
test_target1                                           userrefs  -         -
test_target1/test_source1                              userrefs  -         -
test_target1/test_source1/fs1                          userrefs  -         -
test_target1/test_source1/fs1@test-20101111000000      userrefs  0         -
test_target1/test_source1/fs1@test-20101111000001      userrefs  1         -
test_target1/test_source1/fs1/sub                      userrefs  -         -
test_target1/test_source1/fs1/sub@test-20101111000000  userrefs  0         -
test_target1/test_source1/fs1/sub@test-20101111000001  userrefs  1         -
test_target1/test_source2                              userrefs  -         -
test_target1/test_source2/fs2                          userrefs  -         -
test_target1/test_source2/fs2/sub                      userrefs  -         -
test_target1/test_source2/fs2/sub@test-20101111000000  userrefs  0         -
test_target1/test_source2/fs2/sub@test-20101111000001  userrefs  1         -
""")

        #make sure time handling is correctly. try to make snapshots a year appart and verify that only snapshots mostly 1y old are kept
        with self.subTest("test time checking"):
            with patch('time.strftime', return_value="test-20111111000000"):
                self.assertFalse(ZfsAutobackup("test test_target1 --allow-empty --verbose --no-progress".split(" ")).run())


            time_str="20111112000000" #month in the "future"
            future_timestamp=time_secs=time.mktime(time.strptime(time_str,"%Y%m%d%H%M%S"))
            with patch('time.time', return_value=future_timestamp):
                with patch('time.strftime', return_value="test-20111111000001"):
                    self.assertFalse(ZfsAutobackup("test test_target1 --allow-empty --verbose --keep-source 1y1y --keep-target 1d1y --no-progress".split(" ")).run())


            r=shelltest("zfs list -H -o name -r -t all "+TEST_POOLS)
            self.assertMultiLineEqual(r,"""
test_source1
test_source1/fs1
test_source1/fs1@test-20111111000000
test_source1/fs1@test-20111111000001
test_source1/fs1/sub
test_source1/fs1/sub@test-20111111000000
test_source1/fs1/sub@test-20111111000001
test_source2
test_source2/fs2
test_source2/fs2/sub
test_source2/fs2/sub@test-20111111000000
test_source2/fs2/sub@test-20111111000001
test_source2/fs3
test_source2/fs3/sub
test_target1
test_target1/test_source1
test_target1/test_source1/fs1
test_target1/test_source1/fs1@test-20111111000000
test_target1/test_source1/fs1@test-20111111000001
test_target1/test_source1/fs1/sub
test_target1/test_source1/fs1/sub@test-20111111000000
test_target1/test_source1/fs1/sub@test-20111111000001
test_target1/test_source2
test_target1/test_source2/fs2
test_target1/test_source2/fs2/sub
test_target1/test_source2/fs2/sub@test-20111111000000
test_target1/test_source2/fs2/sub@test-20111111000001
""")

    def  test_ignore_othersnaphots(self):

        r=shelltest("zfs snapshot test_source1/fs1@othersimple")
        r=shelltest("zfs snapshot test_source1/fs1@otherdate-20001111000000")

        with patch('time.strftime', return_value="test-20101111000000"):
            self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose".split(" ")).run())

            r=shelltest("zfs list -H -o name -r -t all "+TEST_POOLS)
            self.assertMultiLineEqual(r,"""
test_source1
test_source1/fs1
test_source1/fs1@othersimple
test_source1/fs1@otherdate-20001111000000
test_source1/fs1@test-20101111000000
test_source1/fs1/sub
test_source1/fs1/sub@test-20101111000000
test_source2
test_source2/fs2
test_source2/fs2/sub
test_source2/fs2/sub@test-20101111000000
test_source2/fs3
test_source2/fs3/sub
test_target1
test_target1/test_source1
test_target1/test_source1/fs1
test_target1/test_source1/fs1@test-20101111000000
test_target1/test_source1/fs1/sub
test_target1/test_source1/fs1/sub@test-20101111000000
test_target1/test_source2
test_target1/test_source2/fs2
test_target1/test_source2/fs2/sub
test_target1/test_source2/fs2/sub@test-20101111000000
""")

    def  test_othersnaphots(self):

        r=shelltest("zfs snapshot test_source1/fs1@othersimple")
        r=shelltest("zfs snapshot test_source1/fs1@otherdate-20001111000000")

        with patch('time.strftime', return_value="test-20101111000000"):
            self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose --other-snapshots".split(" ")).run())

            r=shelltest("zfs list -H -o name -r -t all "+TEST_POOLS)
            self.assertMultiLineEqual(r,"""
test_source1
test_source1/fs1
test_source1/fs1@othersimple
test_source1/fs1@otherdate-20001111000000
test_source1/fs1@test-20101111000000
test_source1/fs1/sub
test_source1/fs1/sub@test-20101111000000
test_source2
test_source2/fs2
test_source2/fs2/sub
test_source2/fs2/sub@test-20101111000000
test_source2/fs3
test_source2/fs3/sub
test_target1
test_target1/test_source1
test_target1/test_source1/fs1
test_target1/test_source1/fs1@othersimple
test_target1/test_source1/fs1@otherdate-20001111000000
test_target1/test_source1/fs1@test-20101111000000
test_target1/test_source1/fs1/sub
test_target1/test_source1/fs1/sub@test-20101111000000
test_target1/test_source2
test_target1/test_source2/fs2
test_target1/test_source2/fs2/sub
test_target1/test_source2/fs2/sub@test-20101111000000
""")


    def  test_nosnapshot(self):

        with patch('time.strftime', return_value="test-20101111000000"):
            self.assertFalse(ZfsAutobackup("test test_target1 --verbose --no-snapshot --no-progress".split(" ")).run())

            r=shelltest("zfs list -H -o name -r -t all "+TEST_POOLS)
            #(only parents are created )
            #TODO: it probably shouldn't create these
            self.assertMultiLineEqual(r,"""
test_source1
test_source1/fs1
test_source1/fs1/sub
test_source2
test_source2/fs2
test_source2/fs2/sub
test_source2/fs3
test_source2/fs3/sub
test_target1
test_target1/test_source1
test_target1/test_source2
test_target1/test_source2/fs2
""")


    def  test_nosend(self):

        with patch('time.strftime', return_value="test-20101111000000"):
            self.assertFalse(ZfsAutobackup("test test_target1 --verbose --no-send --no-progress".split(" ")).run())

            r=shelltest("zfs list -H -o name -r -t all "+TEST_POOLS)
            self.assertMultiLineEqual(r,"""
test_source1
test_source1/fs1
test_source1/fs1@test-20101111000000
test_source1/fs1/sub
test_source1/fs1/sub@test-20101111000000
test_source2
test_source2/fs2
test_source2/fs2/sub
test_source2/fs2/sub@test-20101111000000
test_source2/fs3
test_source2/fs3/sub
test_target1
""")


    def  test_ignorereplicated(self):
        r=shelltest("zfs snapshot test_source1/fs1@otherreplication")

        with patch('time.strftime', return_value="test-20101111000000"):
            self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose --ignore-replicated".split(" ")).run())

            r=shelltest("zfs list -H -o name -r -t all "+TEST_POOLS)
            self.assertMultiLineEqual(r,"""
test_source1
test_source1/fs1
test_source1/fs1@otherreplication
test_source1/fs1/sub
test_source1/fs1/sub@test-20101111000000
test_source2
test_source2/fs2
test_source2/fs2/sub
test_source2/fs2/sub@test-20101111000000
test_source2/fs3
test_source2/fs3/sub
test_target1
test_target1/test_source1
test_target1/test_source1/fs1
test_target1/test_source1/fs1/sub
test_target1/test_source1/fs1/sub@test-20101111000000
test_target1/test_source2
test_target1/test_source2/fs2
test_target1/test_source2/fs2/sub
test_target1/test_source2/fs2/sub@test-20101111000000
""")

    def  test_noholds(self):

        with patch('time.strftime', return_value="test-20101111000000"):
            self.assertFalse(ZfsAutobackup("test test_target1 --verbose --no-holds --no-progress".split(" ")).run())

            r=shelltest("zfs get -r userrefs test_source1 test_source2 test_target1")
            self.assertMultiLineEqual(r,"""
NAME                                                   PROPERTY  VALUE     SOURCE
test_source1                                           userrefs  -         -
test_source1/fs1                                       userrefs  -         -
test_source1/fs1@test-20101111000000                   userrefs  0         -
test_source1/fs1/sub                                   userrefs  -         -
test_source1/fs1/sub@test-20101111000000               userrefs  0         -
test_source2                                           userrefs  -         -
test_source2/fs2                                       userrefs  -         -
test_source2/fs2/sub                                   userrefs  -         -
test_source2/fs2/sub@test-20101111000000               userrefs  0         -
test_source2/fs3                                       userrefs  -         -
test_source2/fs3/sub                                   userrefs  -         -
test_target1                                           userrefs  -         -
test_target1/test_source1                              userrefs  -         -
test_target1/test_source1/fs1                          userrefs  -         -
test_target1/test_source1/fs1@test-20101111000000      userrefs  0         -
test_target1/test_source1/fs1/sub                      userrefs  -         -
test_target1/test_source1/fs1/sub@test-20101111000000  userrefs  0         -
test_target1/test_source2                              userrefs  -         -
test_target1/test_source2/fs2                          userrefs  -         -
test_target1/test_source2/fs2/sub                      userrefs  -         -
test_target1/test_source2/fs2/sub@test-20101111000000  userrefs  0         -
""")


    def  test_strippath(self):

        with patch('time.strftime', return_value="test-20101111000000"):
            self.assertFalse(ZfsAutobackup("test test_target1 --verbose --strip-path=1 --no-progress".split(" ")).run())

            r=shelltest("zfs list -H -o name -r -t all "+TEST_POOLS)
            self.assertMultiLineEqual(r,"""
test_source1
test_source1/fs1
test_source1/fs1@test-20101111000000
test_source1/fs1/sub
test_source1/fs1/sub@test-20101111000000
test_source2
test_source2/fs2
test_source2/fs2/sub
test_source2/fs2/sub@test-20101111000000
test_source2/fs3
test_source2/fs3/sub
test_target1
test_target1/fs1
test_target1/fs1@test-20101111000000
test_target1/fs1/sub
test_target1/fs1/sub@test-20101111000000
test_target1/fs2
test_target1/fs2/sub
test_target1/fs2/sub@test-20101111000000
""")

    def test_strippath_collision(self):
        with self.assertRaisesRegexp(Exception,"collision"):
            ZfsAutobackup("test test_target1 --verbose --strip-path=2 --no-progress --debug".split(" ")).run()

    def test_strippath_toomuch(self):
        with self.assertRaisesRegexp(Exception,"too much"):
            ZfsAutobackup("test test_target1 --verbose --strip-path=3 --no-progress --debug".split(" ")).run()

    def  test_clearrefres(self):

        #on zfs utils 0.6.x -x isnt supported
        r=shelltest("zfs recv -x bla test >/dev/null </dev/zero; echo $?")
        if r=="\n2\n":
            self.skipTest("This zfs-userspace version doesnt support -x")

        r=shelltest("zfs set refreservation=1M test_source1/fs1")

        with patch('time.strftime', return_value="test-20101111000000"):
            self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose --clear-refreservation".split(" ")).run())

            r=shelltest("zfs get refreservation -r test_source1 test_source2 test_target1")
            self.assertMultiLineEqual(r,"""
NAME                                                   PROPERTY        VALUE      SOURCE
test_source1                                           refreservation  none       default
test_source1/fs1                                       refreservation  1M         local
test_source1/fs1@test-20101111000000                   refreservation  -          -
test_source1/fs1/sub                                   refreservation  none       default
test_source1/fs1/sub@test-20101111000000               refreservation  -          -
test_source2                                           refreservation  none       default
test_source2/fs2                                       refreservation  none       default
test_source2/fs2/sub                                   refreservation  none       default
test_source2/fs2/sub@test-20101111000000               refreservation  -          -
test_source2/fs3                                       refreservation  none       default
test_source2/fs3/sub                                   refreservation  none       default
test_target1                                           refreservation  none       default
test_target1/test_source1                              refreservation  none       default
test_target1/test_source1/fs1                          refreservation  none       default
test_target1/test_source1/fs1@test-20101111000000      refreservation  -          -
test_target1/test_source1/fs1/sub                      refreservation  none       default
test_target1/test_source1/fs1/sub@test-20101111000000  refreservation  -          -
test_target1/test_source2                              refreservation  none       default
test_target1/test_source2/fs2                          refreservation  none       default
test_target1/test_source2/fs2/sub                      refreservation  none       default
test_target1/test_source2/fs2/sub@test-20101111000000  refreservation  -          -
""")


    def  test_clearmount(self):

        #on zfs utils 0.6.x -o isnt supported
        r=shelltest("zfs recv -o bla=1 test >/dev/null </dev/zero; echo $?")
        if r=="\n2\n":
            self.skipTest("This zfs-userspace version doesnt support -o")


        with patch('time.strftime', return_value="test-20101111000000"):
            self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose --clear-mountpoint --debug".split(" ")).run())

            r=shelltest("zfs get canmount -r test_source1 test_source2 test_target1")
            self.assertMultiLineEqual(r,"""
NAME                                                   PROPERTY  VALUE     SOURCE
test_source1                                           canmount  on        default
test_source1/fs1                                       canmount  on        default
test_source1/fs1@test-20101111000000                   canmount  -         -
test_source1/fs1/sub                                   canmount  on        default
test_source1/fs1/sub@test-20101111000000               canmount  -         -
test_source2                                           canmount  on        default
test_source2/fs2                                       canmount  on        default
test_source2/fs2/sub                                   canmount  on        default
test_source2/fs2/sub@test-20101111000000               canmount  -         -
test_source2/fs3                                       canmount  on        default
test_source2/fs3/sub                                   canmount  on        default
test_target1                                           canmount  on        default
test_target1/test_source1                              canmount  on        default
test_target1/test_source1/fs1                          canmount  noauto    local
test_target1/test_source1/fs1@test-20101111000000      canmount  -         -
test_target1/test_source1/fs1/sub                      canmount  noauto    local
test_target1/test_source1/fs1/sub@test-20101111000000  canmount  -         -
test_target1/test_source2                              canmount  on        default
test_target1/test_source2/fs2                          canmount  on        default
test_target1/test_source2/fs2/sub                      canmount  noauto    local
test_target1/test_source2/fs2/sub@test-20101111000000  canmount  -         -
""")


    def  test_rollback(self):

        #initial backup
        with patch('time.strftime', return_value="test-20101111000000"):
            self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose".split(" ")).run())

        #make change
        r=shelltest("zfs mount test_target1/test_source1/fs1")
        r=shelltest("touch /test_target1/test_source1/fs1/change.txt")

        with patch('time.strftime', return_value="test-20101111000001"):
            #should fail (busy)
            self.assertTrue(ZfsAutobackup("test test_target1 --no-progress --verbose --allow-empty".split(" ")).run())

        with patch('time.strftime', return_value="test-20101111000002"):
            #rollback, should succeed
            self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose --allow-empty --rollback".split(" ")).run())


    def  test_destroyincompat(self):

        #initial backup
        with patch('time.strftime', return_value="test-20101111000000"):
            self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose".split(" ")).run())

        #add multiple compatible snapshot (written is still 0)
        r=shelltest("zfs snapshot test_target1/test_source1/fs1@compatible1")
        r=shelltest("zfs snapshot test_target1/test_source1/fs1@compatible2")

        with patch('time.strftime', return_value="test-20101111000001"):
            #should be ok, is compatible
            self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose --allow-empty".split(" ")).run())

        #add incompatible snapshot by changing and snapshotting
        r=shelltest("zfs mount test_target1/test_source1/fs1")
        r=shelltest("touch /test_target1/test_source1/fs1/change.txt")
        r=shelltest("zfs snapshot test_target1/test_source1/fs1@incompatible1")


        with patch('time.strftime', return_value="test-20101111000002"):
            #--test should fail, now incompatible
            self.assertTrue(ZfsAutobackup("test test_target1 --no-progress --verbose --allow-empty --test".split(" ")).run())

        with patch('time.strftime', return_value="test-20101111000002"):
            #should fail, now incompatible
            self.assertTrue(ZfsAutobackup("test test_target1 --no-progress --verbose --allow-empty".split(" ")).run())

        with patch('time.strftime', return_value="test-20101111000003"):
            #--test should succeed by destroying incompatibles
            self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose --allow-empty --destroy-incompatible --test".split(" ")).run())

        with patch('time.strftime', return_value="test-20101111000003"):
            #should succeed by destroying incompatibles
            self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose --allow-empty --destroy-incompatible".split(" ")).run())

        r = shelltest("zfs list -H -o name -r -t all test_target1")
        self.assertMultiLineEqual(r, """
test_target1
test_target1/test_source1
test_target1/test_source1/fs1
test_target1/test_source1/fs1@test-20101111000000
test_target1/test_source1/fs1@compatible1
test_target1/test_source1/fs1@compatible2
test_target1/test_source1/fs1@test-20101111000001
test_target1/test_source1/fs1@test-20101111000002
test_target1/test_source1/fs1@test-20101111000003
test_target1/test_source1/fs1/sub
test_target1/test_source1/fs1/sub@test-20101111000000
test_target1/test_source1/fs1/sub@test-20101111000001
test_target1/test_source1/fs1/sub@test-20101111000002
test_target1/test_source1/fs1/sub@test-20101111000003
test_target1/test_source2
test_target1/test_source2/fs2
test_target1/test_source2/fs2/sub
test_target1/test_source2/fs2/sub@test-20101111000000
test_target1/test_source2/fs2/sub@test-20101111000001
test_target1/test_source2/fs2/sub@test-20101111000002
test_target1/test_source2/fs2/sub@test-20101111000003
""")






    def test_ssh(self):

        #test all ssh directions

        with patch('time.strftime', return_value="test-20101111000000"):
            self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose --allow-empty --ssh-source localhost --exclude-received".split(" ")).run())

        with patch('time.strftime', return_value="test-20101111000001"):
            self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose --allow-empty --ssh-target localhost --exclude-received".split(" ")).run())

        with patch('time.strftime', return_value="test-20101111000002"):
            self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose --allow-empty --ssh-source localhost --ssh-target localhost".split(" ")).run())


        r=shelltest("zfs list -H -o name -r -t all "+TEST_POOLS)
        self.assertMultiLineEqual(r,"""
test_source1
test_source1/fs1
test_source1/fs1@test-20101111000000
test_source1/fs1@test-20101111000001
test_source1/fs1@test-20101111000002
test_source1/fs1/sub
test_source1/fs1/sub@test-20101111000000
test_source1/fs1/sub@test-20101111000001
test_source1/fs1/sub@test-20101111000002
test_source2
test_source2/fs2
test_source2/fs2/sub
test_source2/fs2/sub@test-20101111000000
test_source2/fs2/sub@test-20101111000001
test_source2/fs2/sub@test-20101111000002
test_source2/fs3
test_source2/fs3/sub
test_target1
test_target1/test_source1
test_target1/test_source1/fs1
test_target1/test_source1/fs1@test-20101111000000
test_target1/test_source1/fs1@test-20101111000001
test_target1/test_source1/fs1@test-20101111000002
test_target1/test_source1/fs1/sub
test_target1/test_source1/fs1/sub@test-20101111000000
test_target1/test_source1/fs1/sub@test-20101111000001
test_target1/test_source1/fs1/sub@test-20101111000002
test_target1/test_source2
test_target1/test_source2/fs2
test_target1/test_source2/fs2/sub
test_target1/test_source2/fs2/sub@test-20101111000000
test_target1/test_source2/fs2/sub@test-20101111000001
test_target1/test_source2/fs2/sub@test-20101111000002
""")


    def  test_minchange(self):

        #initial
        with patch('time.strftime', return_value="test-20101111000000"):
            self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose --min-change 100000".split(" ")).run())

        #make small change, use umount to reflect the changes immediately
        r=shelltest("zfs set compress=off test_source1")
        r=shelltest("touch /test_source1/fs1/change.txt")
        r=shelltest("zfs umount test_source1/fs1; zfs mount test_source1/fs1")


        #too small change, takes no snapshots
        with patch('time.strftime', return_value="test-20101111000001"):
            self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose --min-change 100000".split(" ")).run())

        #make big change
        r=shelltest("dd if=/dev/zero of=/test_source1/fs1/change.txt bs=200000 count=1")
        r=shelltest("zfs umount test_source1/fs1; zfs mount test_source1/fs1")

        #bigger change, should take snapshot
        with patch('time.strftime', return_value="test-20101111000002"):
            self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose --min-change 100000".split(" ")).run())

        r=shelltest("zfs list -H -o name -r -t all "+TEST_POOLS)
        self.assertMultiLineEqual(r,"""
test_source1
test_source1/fs1
test_source1/fs1@test-20101111000000
test_source1/fs1@test-20101111000002
test_source1/fs1/sub
test_source1/fs1/sub@test-20101111000000
test_source2
test_source2/fs2
test_source2/fs2/sub
test_source2/fs2/sub@test-20101111000000
test_source2/fs3
test_source2/fs3/sub
test_target1
test_target1/test_source1
test_target1/test_source1/fs1
test_target1/test_source1/fs1@test-20101111000000
test_target1/test_source1/fs1@test-20101111000002
test_target1/test_source1/fs1/sub
test_target1/test_source1/fs1/sub@test-20101111000000
test_target1/test_source2
test_target1/test_source2/fs2
test_target1/test_source2/fs2/sub
test_target1/test_source2/fs2/sub@test-20101111000000
""")

    def  test_test(self):

        #initial
        with patch('time.strftime', return_value="test-20101111000000"):
            self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose --test".split(" ")).run())

        r=shelltest("zfs list -H -o name -r -t all "+TEST_POOLS)
        self.assertMultiLineEqual(r,"""
test_source1
test_source1/fs1
test_source1/fs1/sub
test_source2
test_source2/fs2
test_source2/fs2/sub
test_source2/fs3
test_source2/fs3/sub
test_target1
""")

        #actual make initial backup
        with patch('time.strftime', return_value="test-20101111000001"):
            self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose".split(" ")).run())


        #test incremental
        with patch('time.strftime', return_value="test-20101111000002"):
            self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --allow-empty --verbose --test".split(" ")).run())

        r=shelltest("zfs list -H -o name -r -t all "+TEST_POOLS)
        self.assertMultiLineEqual(r,"""
test_source1
test_source1/fs1
test_source1/fs1@test-20101111000001
test_source1/fs1/sub
test_source1/fs1/sub@test-20101111000001
test_source2
test_source2/fs2
test_source2/fs2/sub
test_source2/fs2/sub@test-20101111000001
test_source2/fs3
test_source2/fs3/sub
test_target1
test_target1/test_source1
test_target1/test_source1/fs1
test_target1/test_source1/fs1@test-20101111000001
test_target1/test_source1/fs1/sub
test_target1/test_source1/fs1/sub@test-20101111000001
test_target1/test_source2
test_target1/test_source2/fs2
test_target1/test_source2/fs2/sub
test_target1/test_source2/fs2/sub@test-20101111000001
""")


    def test_migrate(self):
        """test migration from other snapshotting systems. zfs-autobackup should be able to continue from any common snapshot, not just its own."""

        shelltest("zfs snapshot test_source1/fs1@migrate1")
        shelltest("zfs create test_target1/test_source1")
        shelltest("zfs send  test_source1/fs1@migrate1| zfs recv test_target1/test_source1/fs1")

        with patch('time.strftime', return_value="test-20101111000000"):
            self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose".split(" ")).run())

        r=shelltest("zfs list -H -o name -r -t all "+TEST_POOLS)
        self.assertMultiLineEqual(r,"""
test_source1
test_source1/fs1
test_source1/fs1@migrate1
test_source1/fs1@test-20101111000000
test_source1/fs1/sub
test_source1/fs1/sub@test-20101111000000
test_source2
test_source2/fs2
test_source2/fs2/sub
test_source2/fs2/sub@test-20101111000000
test_source2/fs3
test_source2/fs3/sub
test_target1
test_target1/test_source1
test_target1/test_source1/fs1
test_target1/test_source1/fs1@migrate1
test_target1/test_source1/fs1@test-20101111000000
test_target1/test_source1/fs1/sub
test_target1/test_source1/fs1/sub@test-20101111000000
test_target1/test_source2
test_target1/test_source2/fs2
test_target1/test_source2/fs2/sub
test_target1/test_source2/fs2/sub@test-20101111000000
""")

    def test_keep0(self):
        """test if keep-source=0 and keep-target=0 dont delete common snapshot and break backup"""

        with patch('time.strftime', return_value="test-20101111000000"):
            self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose --keep-source=0 --keep-target=0".split(" ")).run())

        #make snapshot, shouldnt delete 0
        with patch('time.strftime', return_value="test-20101111000001"):
            self.assertFalse(ZfsAutobackup("test --no-progress --verbose --keep-source=0 --keep-target=0 --allow-empty".split(" ")).run())

        #make snapshot 2, shouldnt delete 0 since it has holds, but will delete 1 since it has no holds
        with patch('time.strftime', return_value="test-20101111000002"):
            self.assertFalse(ZfsAutobackup("test --no-progress --verbose --keep-source=0 --keep-target=0 --allow-empty".split(" ")).run())

        r = shelltest("zfs list -H -o name -r -t all " + TEST_POOLS)
        self.assertMultiLineEqual(r, """
test_source1
test_source1/fs1
test_source1/fs1@test-20101111000000
test_source1/fs1@test-20101111000002
test_source1/fs1/sub
test_source1/fs1/sub@test-20101111000000
test_source1/fs1/sub@test-20101111000002
test_source2
test_source2/fs2
test_source2/fs2/sub
test_source2/fs2/sub@test-20101111000000
test_source2/fs2/sub@test-20101111000002
test_source2/fs3
test_source2/fs3/sub
test_target1
test_target1/test_source1
test_target1/test_source1/fs1
test_target1/test_source1/fs1@test-20101111000000
test_target1/test_source1/fs1/sub
test_target1/test_source1/fs1/sub@test-20101111000000
test_target1/test_source2
test_target1/test_source2/fs2
test_target1/test_source2/fs2/sub
test_target1/test_source2/fs2/sub@test-20101111000000
""")

        #make another backup but with no-holds. we should naturally endup with only number 3
        with patch('time.strftime', return_value="test-20101111000003"):
            self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose --keep-source=0 --keep-target=0 --no-holds --allow-empty".split(" ")).run())

        r = shelltest("zfs list -H -o name -r -t all " + TEST_POOLS)
        self.assertMultiLineEqual(r, """
test_source1
test_source1/fs1
test_source1/fs1@test-20101111000003
test_source1/fs1/sub
test_source1/fs1/sub@test-20101111000003
test_source2
test_source2/fs2
test_source2/fs2/sub
test_source2/fs2/sub@test-20101111000003
test_source2/fs3
test_source2/fs3/sub
test_target1
test_target1/test_source1
test_target1/test_source1/fs1
test_target1/test_source1/fs1@test-20101111000003
test_target1/test_source1/fs1/sub
test_target1/test_source1/fs1/sub@test-20101111000003
test_target1/test_source2
test_target1/test_source2/fs2
test_target1/test_source2/fs2/sub
test_target1/test_source2/fs2/sub@test-20101111000003
""")


        # run with snapshot-only for 4, since we used no-holds, it will delete 3 on the source, breaking the backup
        with patch('time.strftime', return_value="test-20101111000004"):
            self.assertFalse(ZfsAutobackup("test --no-progress --verbose --keep-source=0 --keep-target=0 --allow-empty".split(" ")).run())

        r = shelltest("zfs list -H -o name -r -t all " + TEST_POOLS)
        self.assertMultiLineEqual(r, """
test_source1
test_source1/fs1
test_source1/fs1@test-20101111000004
test_source1/fs1/sub
test_source1/fs1/sub@test-20101111000004
test_source2
test_source2/fs2
test_source2/fs2/sub
test_source2/fs2/sub@test-20101111000004
test_source2/fs3
test_source2/fs3/sub
test_target1
test_target1/test_source1
test_target1/test_source1/fs1
test_target1/test_source1/fs1@test-20101111000003
test_target1/test_source1/fs1/sub
test_target1/test_source1/fs1/sub@test-20101111000003
test_target1/test_source2
test_target1/test_source2/fs2
test_target1/test_source2/fs2/sub
test_target1/test_source2/fs2/sub@test-20101111000003
""")


    def test_progress(self):

        r=shelltest("dd if=/dev/zero of=/test_source1/data.txt bs=200000 count=1")
        r = shelltest("zfs snapshot test_source1@test")

        l=LogConsole(show_verbose=True, show_debug=False, color=False)
        n=ZfsNode(snapshot_time_format="bla", hold_name="bla", logger=l)
        d=ZfsDataset(n,"test_source1@test")

        sp=d.send_pipe([], prev_snapshot=None, resume_token=None, show_progress=True, raw=False, send_pipes=[], send_properties=True, write_embedded=True, zfs_compressed=True)


        with OutputIO() as buf:
            with redirect_stderr(buf):
                try:
                    n.run(["sleep", "2"], inp=sp)
                except:
                    pass

            print(buf.getvalue())
            # correct message?
            self.assertRegex(buf.getvalue(),".*>>> .*minutes left.*")
