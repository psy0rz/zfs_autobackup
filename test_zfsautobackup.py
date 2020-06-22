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

            r=shelltest("zfs list -H -o name -r -t all")
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

        
            r=shelltest("zfs list -H -o name -r -t all")
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

            r=shelltest("zfs list -H -o name -r -t all")
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

            r=shelltest("zfs list -H -o name -r -t all")
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

            r=shelltest("zfs list -H -o name -r -t all")
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

            r=shelltest("zfs list -H -o name -r -t all")
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

            r=shelltest("zfs list -H -o name -r -t all")
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

            r=shelltest("zfs list -H -o name -r -t all")
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

        #on zfs utils 0.6.x this isnt supported, skip for now:
        r=shelltest("zfs recv -x bla test &>/dev/null; echo  $?")
        if r=="\n2\n":
            pass

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

        #on zfs utils 0.6.x this isnt supported, skip for now:
        r=shelltest("zfs recv -o bla=1 test; echo $?")
        if r=="\n2\n":
            pass


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



