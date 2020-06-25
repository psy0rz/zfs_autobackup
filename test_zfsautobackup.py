from basetest import *
import time



class TestZfsAutobackup(unittest2.TestCase):
    
    def setUp(self):
        prepare_zpools()
        self.longMessage=True

    def  test_defaults(self):

        with self.subTest("defaults with full verbose and debug"):

            with patch('time.strftime', return_value="20101111000000"):
                self.assertFalse(ZfsAutobackup("test test_target1 --verbose --debug".split(" ")).run())

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
            with patch('time.strftime', return_value="20101111000001"):
                self.assertFalse(ZfsAutobackup("test test_target1 --allow-empty".split(" ")).run())

        
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




    def  test_ignore_othersnaphots(self):

        r=shelltest("zfs snapshot test_source1/fs1@othersimple")
        r=shelltest("zfs snapshot test_source1/fs1@otherdate-20001111000000")

        with patch('time.strftime', return_value="20101111000000"):
            self.assertFalse(ZfsAutobackup("test test_target1 --verbose".split(" ")).run())

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

        with patch('time.strftime', return_value="20101111000000"):
            self.assertFalse(ZfsAutobackup("test test_target1 --verbose --other-snapshots".split(" ")).run())

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

        with patch('time.strftime', return_value="20101111000000"):
            self.assertFalse(ZfsAutobackup("test test_target1 --verbose --no-snapshot".split(" ")).run())

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

        with patch('time.strftime', return_value="20101111000000"):
            self.assertFalse(ZfsAutobackup("test test_target1 --verbose --no-send".split(" ")).run())

            r=shelltest("zfs list -H -o name -r -t all "+TEST_POOLS)
            #(only parents are created )
            #TODO: it probably shouldn't create these
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

        with patch('time.strftime', return_value="20101111000000"):
            self.assertFalse(ZfsAutobackup("test test_target1 --verbose --ignore-replicated".split(" ")).run())

            r=shelltest("zfs list -H -o name -r -t all "+TEST_POOLS)
            #(only parents are created )
            #TODO: it probably shouldn't create these
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

        with patch('time.strftime', return_value="20101111000000"):
            self.assertFalse(ZfsAutobackup("test test_target1 --verbose --no-holds".split(" ")).run())

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
test_target1/test_source1/fs1@test-20101111000000      userrefs  1         -
test_target1/test_source1/fs1/sub                      userrefs  -         -
test_target1/test_source1/fs1/sub@test-20101111000000  userrefs  1         -
test_target1/test_source2                              userrefs  -         -
test_target1/test_source2/fs2                          userrefs  -         -
test_target1/test_source2/fs2/sub                      userrefs  -         -
test_target1/test_source2/fs2/sub@test-20101111000000  userrefs  1         -
""")


    def  test_strippath(self):

        with patch('time.strftime', return_value="20101111000000"):
            self.assertFalse(ZfsAutobackup("test test_target1 --verbose --strip-path=1".split(" ")).run())

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


    def  test_clearrefres(self):

        #on zfs utils 0.6.x -x isnt supported 
        r=shelltest("zfs recv -x bla test >/dev/null </dev/zero; echo $?")
        if r=="\n2\n":
            self.skipTest("This zfs-userspace version doesnt support -x")

        r=shelltest("zfs set refreservation=1M test_source1/fs1")

        with patch('time.strftime', return_value="20101111000000"):
            self.assertFalse(ZfsAutobackup("test test_target1 --verbose --clear-refreservation".split(" ")).run())

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


        with patch('time.strftime', return_value="20101111000000"):
            self.assertFalse(ZfsAutobackup("test test_target1 --verbose --clear-mountpoint".split(" ")).run())

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
        with patch('time.strftime', return_value="20101111000000"):
            self.assertFalse(ZfsAutobackup("test test_target1 --verbose".split(" ")).run())

        #make change
        r=shelltest("zfs mount test_target1/test_source1/fs1")
        r=shelltest("touch /test_target1/test_source1/fs1/change.txt")

        with patch('time.strftime', return_value="20101111000001"):
            #should fail (busy)
            self.assertTrue(ZfsAutobackup("test test_target1 --verbose --allow-empty".split(" ")).run())

        with patch('time.strftime', return_value="20101111000002"):
            #rollback, should succeed
            self.assertFalse(ZfsAutobackup("test test_target1 --verbose --allow-empty --rollback".split(" ")).run())


    def  test_destroyincompat(self):

        #initial backup
        with patch('time.strftime', return_value="20101111000000"):
            self.assertFalse(ZfsAutobackup("test test_target1 --verbose".split(" ")).run())

        #add multiple compatible snapshot (written is still 0)
        r=shelltest("zfs snapshot test_target1/test_source1/fs1@compatible1")
        r=shelltest("zfs snapshot test_target1/test_source1/fs1@compatible2")

        with patch('time.strftime', return_value="20101111000001"):
            #should be ok, is compatible
            self.assertFalse(ZfsAutobackup("test test_target1 --verbose --allow-empty".split(" ")).run())

        #add incompatible snapshot by changing and snapshotting
        r=shelltest("zfs mount test_target1/test_source1/fs1")
        r=shelltest("touch /test_target1/test_source1/fs1/change.txt")
        r=shelltest("zfs snapshot test_target1/test_source1/fs1@incompatible1")

        with patch('time.strftime', return_value="20101111000002"):
            #should fail, now incompatible
            self.assertTrue(ZfsAutobackup("test test_target1 --verbose --allow-empty".split(" ")).run())


        with patch('time.strftime', return_value="20101111000003"):
            #should succeed by destroying incompatibles
            self.assertFalse(ZfsAutobackup("test test_target1 --verbose --allow-empty --destroy-incompatible".split(" ")).run())





    def test_keepsourcetarget(self):

        with patch('time.strftime', return_value="20101111000000"):
            self.assertFalse(ZfsAutobackup("test test_target1 --verbose --allow-empty".split(" ")).run())

        with patch('time.strftime', return_value="20101111000001"):
            self.assertFalse(ZfsAutobackup("test test_target1 --verbose --allow-empty".split(" ")).run())

        #should still have all snapshots
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


        #run again with keep=0
        with patch('time.strftime', return_value="20101111000002"):
            self.assertFalse(ZfsAutobackup("test test_target1 --verbose --allow-empty --keep-source=0 --keep-target=0".split(" ")).run())

        #should only have last snapshots
        r=shelltest("zfs list -H -o name -r -t all "+TEST_POOLS)
        self.assertMultiLineEqual(r,"""
test_source1
test_source1/fs1
test_source1/fs1@test-20101111000002
test_source1/fs1/sub
test_source1/fs1/sub@test-20101111000002
test_source2
test_source2/fs2
test_source2/fs2/sub
test_source2/fs2/sub@test-20101111000002
test_source2/fs3
test_source2/fs3/sub
test_target1
test_target1/test_source1
test_target1/test_source1/fs1
test_target1/test_source1/fs1@test-20101111000002
test_target1/test_source1/fs1/sub
test_target1/test_source1/fs1/sub@test-20101111000002
test_target1/test_source2
test_target1/test_source2/fs2
test_target1/test_source2/fs2/sub
test_target1/test_source2/fs2/sub@test-20101111000002
""")


    def test_ssh(self):

        #test all ssh directions

        with patch('time.strftime', return_value="20101111000000"):
            self.assertFalse(ZfsAutobackup("test test_target1 --verbose --allow-empty --ssh-source localhost".split(" ")).run())

        with patch('time.strftime', return_value="20101111000001"):
            self.assertFalse(ZfsAutobackup("test test_target1 --verbose --allow-empty --ssh-target localhost".split(" ")).run())

        with patch('time.strftime', return_value="20101111000002"):
            self.assertFalse(ZfsAutobackup("test test_target1 --verbose --allow-empty --ssh-source localhost --ssh-target localhost".split(" ")).run())


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
        with patch('time.strftime', return_value="20101111000000"):
            self.assertFalse(ZfsAutobackup("test test_target1 --verbose --min-change 100000".split(" ")).run())

        #make small change, use umount to reflect the changes immediately
        r=shelltest("zfs set compress=off test_source1")
        r=shelltest("touch /test_source1/fs1/change.txt")
        r=shelltest("zfs umount test_source1/fs1; zfs mount test_source1/fs1")
        

        #too small change, takes no snapshots
        with patch('time.strftime', return_value="20101111000001"):
            self.assertFalse(ZfsAutobackup("test test_target1 --verbose --min-change 100000".split(" ")).run())

        #make big change
        r=shelltest("dd if=/dev/zero of=/test_source1/fs1/change.txt bs=200000 count=1")
        r=shelltest("zfs umount test_source1/fs1; zfs mount test_source1/fs1")

        #bigger change, should take snapshot
        with patch('time.strftime', return_value="20101111000002"):
            self.assertFalse(ZfsAutobackup("test test_target1 --verbose --min-change 100000".split(" ")).run())

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

        with patch('time.strftime', return_value="20101111000000"):
            self.assertFalse(ZfsAutobackup("test test_target1 --verbose --test".split(" ")).run())

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


###########################
# TODO:

    def  test_raw(self):
        
        self.skipTest("todo: later when travis supports zfs 0.8")

    def  test_ignoretransfererrors(self):
        
        self.skipTest("todo: create some kind of situation where zfs recv exits with an error but transfer is still ok (happens in practice with acltype)")
