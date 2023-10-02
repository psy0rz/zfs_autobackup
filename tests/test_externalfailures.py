from basetest import *


class TestExternalFailures(unittest2.TestCase):

    def setUp(self):
        prepare_zpools()
        self.longMessage = True

    # generate a resumable state
    # NOTE: this generates two resumable test_target1/test_source1/fs1 and test_target1/test_source1/fs1/sub
    def generate_resume(self):

        r = shelltest("zfs set compress=off test_source1 test_target1")

        # big change on source
        r = shelltest("dd if=/dev/zero of=/test_source1/fs1/data bs=250M count=1")

        # waste space on target
        r = shelltest("dd if=/dev/zero of=/test_target1/waste bs=250M count=1")

        # should fail and leave resume token (if supported)
        self.assertTrue(ZfsAutobackup("test test_target1 --no-progress --verbose".split(" ")).run())

        # free up space
        r = shelltest("rm /test_target1/waste")
        # sync
        r = shelltest("zfs umount test_target1")
        r = shelltest("zfs mount test_target1")

    # resume initial backup
    def test_initial_resume(self):

        # inital backup, leaves resume token
        with mocktime("20101111000000"):
            self.generate_resume()

        # --test should resume and succeed
        with OutputIO() as buf:
            with redirect_stdout(buf):
                self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose --test".split(" ")).run())

            print(buf.getvalue())

            self.assertIn(": resuming", buf.getvalue())

        # should resume and succeed
        with OutputIO() as buf:
            with redirect_stdout(buf):
                self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose".split(" ")).run())

            print(buf.getvalue())

            self.assertIn(": resuming", buf.getvalue())

        r = shelltest("zfs list -H -o name -r -t all test_target1")
        self.assertMultiLineEqual(r, """
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

    # resume incremental backup
    def test_incremental_resume(self):

        # initial backup
        with mocktime("20101111000000"):
            self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose --allow-empty".split(" ")).run())

        # incremental backup leaves resume token
        with mocktime("20101111000001"):
            self.generate_resume()

        # --test should resume and succeed
        with OutputIO() as buf:
            with redirect_stdout(buf):
                self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose --test".split(" ")).run())

            print(buf.getvalue())

            self.assertIn(": resuming", buf.getvalue())

        # should resume and succeed
        with OutputIO() as buf:
            with redirect_stdout(buf):
                self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose".split(" ")).run())

            print(buf.getvalue())

            # did we really resume?
            self.assertIn(": resuming", buf.getvalue())

        r = shelltest("zfs list -H -o name -r -t all test_target1")
        self.assertMultiLineEqual(r, """
test_target1
test_target1/test_source1
test_target1/test_source1/fs1
test_target1/test_source1/fs1@test-20101111000000
test_target1/test_source1/fs1@test-20101111000001
test_target1/test_source1/fs1/sub
test_target1/test_source1/fs1/sub@test-20101111000000
test_target1/test_source2
test_target1/test_source2/fs2
test_target1/test_source2/fs2/sub
test_target1/test_source2/fs2/sub@test-20101111000000
""")

    # generate an invalid resume token, and verify if its aborted automaticly
    def test_initial_resumeabort(self):


        # inital backup, leaves resume token
        with mocktime("20101111000000"):
            self.generate_resume()

        # remove corresponding source snapshot, so it becomes invalid
        shelltest("zfs destroy test_source1/fs1@test-20101111000000")

        # NOTE: it can only abort the initial dataset if it has no subs
        shelltest("zfs destroy test_target1/test_source1/fs1/sub; true")

        # --test try again, should abort old resume
        with mocktime("20101111000001"):
            self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose --test".split(" ")).run())

        # try again, should abort old resume
        with mocktime("20101111000001"):
            self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose".split(" ")).run())

        r = shelltest("zfs list -H -o name -r -t all test_target1")
        self.assertMultiLineEqual(r, """
test_target1
test_target1/test_source1
test_target1/test_source1/fs1
test_target1/test_source1/fs1@test-20101111000001
test_target1/test_source1/fs1/sub
test_target1/test_source1/fs1/sub@test-20101111000000
test_target1/test_source2
test_target1/test_source2/fs2
test_target1/test_source2/fs2/sub
test_target1/test_source2/fs2/sub@test-20101111000000
""")

    # generate an invalid resume token, and verify if its aborted automaticly
    def test_incremental_resumeabort(self):

        # initial backup
        with mocktime("20101111000000"):
            self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose --allow-empty".split(" ")).run())

        # icremental backup, leaves resume token
        with mocktime("20101111000001"):
            self.generate_resume()

        # remove corresponding source snapshot, so it becomes invalid
        shelltest("zfs destroy test_source1/fs1@test-20101111000001")

        # --test try again, should abort old resume
        with mocktime("20101111000002"):
            self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose --test".split(" ")).run())

        # try again, should abort old resume
        with mocktime("20101111000002"):
            self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose".split(" ")).run())

        r = shelltest("zfs list -H -o name -r -t all test_target1")
        self.assertMultiLineEqual(r, """
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

    # create a resume situation, where the other side doesnt want the snapshot anymore ( should abort resume )
    def test_abort_unwanted_resume(self):

        with mocktime("20101111000000"):
            self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose".split(" ")).run())

        # generate resume
        with mocktime("20101111000001"):
            self.generate_resume()

        with OutputIO() as buf:
            with redirect_stdout(buf):
                # incremental, doesnt want previous anymore
                with mocktime("20101111000002"):
                    self.assertFalse(ZfsAutobackup(
                        "test test_target1 --no-progress --verbose --keep-target=0 --allow-empty --debug".split(" ")).run())

            print(buf.getvalue())

            self.assertIn("Aborting resume, we dont want that snapshot anymore.", buf.getvalue())

        r = shelltest("zfs list -H -o name -r -t all test_target1")
        self.assertMultiLineEqual(r, """
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

    # test with empty snapshot list (this was a bug)
    def test_abort_resume_emptysnapshotlist(self):

        with mocktime("20101111000000"):
            self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose".split(" ")).run())

        # generate resume
        with mocktime("20101111000001"):
            self.generate_resume()

        shelltest("zfs destroy test_source1/fs1@test-20101111000001")

        with OutputIO() as buf:
            with redirect_stdout(buf):
                # incremental, doesnt want previous anymore
                with mocktime("20101111000002"):
                    self.assertFalse(ZfsAutobackup(
                        "test test_target1 --no-progress --verbose --no-snapshot".split(
                            " ")).run())

            print(buf.getvalue())

            self.assertIn("Aborting resume, its obsolete", buf.getvalue())


    def test_missing_common(self):

        with mocktime("20101111000000"):
            self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose --allow-empty".split(" ")).run())

        # remove common snapshot and leave nothing
        shelltest("zfs release zfs_autobackup:test test_source1/fs1@test-20101111000000")
        shelltest("zfs destroy test_source1/fs1@test-20101111000000")

        with mocktime("20101111000001"):
            self.assertTrue(ZfsAutobackup("test test_target1 --no-progress --verbose --allow-empty".split(" ")).run())

    #UPDATE: offcourse the one thing that wasn't tested had a bug :(  (in ExecuteNode.run()).
    def test_ignoretransfererrors(self):

            self.skipTest("Not sure how to implement a test for this without some serious hacking and patching.")

#         #recreate target pool without any features
#         # shelltest("zfs set compress=on test_source1; zpool destroy test_target1; zpool create test_target1 -o feature@project_quota=disabled /dev/ram2")
#
#         with mocktime("20101111000000"):
#             self.assertFalse(ZfsAutobackup("test test_target1 --verbose --allow-empty --no-progress".split(" ")).run())
#
#         r = shelltest("zfs list -H -o name -r -t all test_target1")
#
#         self.assertMultiLineEqual(r, """
# test_target1
# test_target1/test_source1
# test_target1/test_source1/fs1
# test_target1/test_source1/fs1@test-20101111000002
# test_target1/test_source1/fs1/sub
# test_target1/test_source1/fs1/sub@test-20101111000002
# test_target1/test_source2
# test_target1/test_source2/fs2
# test_target1/test_source2/fs2/sub
# test_target1/test_source2/fs2/sub@test-20101111000002
#         """)
