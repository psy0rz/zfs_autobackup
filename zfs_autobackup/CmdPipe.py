import subprocess
import os
import select

try:
    from shlex import quote as cmd_quote
except ImportError:
    from pipes import quote as cmd_quote


class CmdItem:
    """one command item, to be added to a CmdPipe"""

    def __init__(self, cmd, readonly=False, stderr_handler=None, exit_handler=None, shell=False):
        """create item. caller has to make sure cmd is properly escaped when using shell.
        :type cmd: list of str
        """

        self.cmd = cmd
        self.readonly = readonly
        self.stderr_handler = stderr_handler
        self.exit_handler = exit_handler
        self.shell = shell
        self.process = None

    def __str__(self):
        """return copy-pastable version of command."""
        if self.shell:
            # its already copy pastable for a shell:
            return " ".join(self.cmd)
        else:
            # make it copy-pastable, will make a mess of quotes sometimes, but is correct
            return " ".join(map(cmd_quote, self.cmd))

    def create(self, stdin):
        """actually create the subprocess (called by CmdPipe)"""

        # make sure the command gets all the data in utf8 format:
        # (this is necessary if LC_ALL=en_US.utf8 is not set in the environment)
        encoded_cmd = []
        for arg in self.cmd:
            encoded_cmd.append(arg.encode('utf-8'))

        self.process = subprocess.Popen(encoded_cmd, env=os.environ, stdout=subprocess.PIPE, stdin=stdin,
                                        stderr=subprocess.PIPE, shell=self.shell)


class CmdPipe:
    """a pipe of one or more commands. also takes care of utf-8 encoding/decoding and line based parsing"""

    def __init__(self, readonly=False, inp=None):
        """
        :param inp: input string for stdin
        :param readonly: Only execute if entire pipe consist of readonly commands
        """
        # list of commands + error handlers to execute
        self.items = []

        self.inp = inp
        self.readonly = readonly
        self._should_execute = True

    def add(self, cmd_item):
        """adds a CmdItem to pipe.
        :type cmd_item: CmdItem
        """

        self.items.append(cmd_item)

        if not cmd_item.readonly and self.readonly:
            self._should_execute = False

    def __str__(self):
        """transform whole pipe into oneliner for debugging and testing. this should generate a copy-pastable string for in a console """

        ret = ""
        for item in self.items:
            if ret:
                ret = ret + " | "
            ret = ret + "({})".format(item)  # this will do proper escaping to make it copypastable

        return ret

    def should_execute(self):
        return self._should_execute

    def execute(self, stdout_handler):
        """run the pipe. returns True all exit handlers returned true"""

        if not self._should_execute:
            return True

        # first process should have actual user input as stdin:
        selectors = []

        # create processes
        last_stdout = None
        stdin = subprocess.PIPE
        for item in self.items:

            item.create(stdin)
            selectors.append(item.process.stderr)

            if last_stdout is None:
                # we're the first process in the pipe, do we have some input?
                if self.inp is not None:
                    # TODO: make streaming to support big inputs?
                    item.process.stdin.write(self.inp.encode('utf-8'))
                item.process.stdin.close()
            else:
                # last stdout was piped to this stdin already, so close it because we dont need it anymore
                last_stdout.close()

            last_stdout = item.process.stdout
            stdin = last_stdout

        # monitor last stdout as well
        selectors.append(last_stdout)

        while True:
            # wait for output on one of the stderrs or last_stdout
            (read_ready, write_ready, ex_ready) = select.select(selectors, [], [])
            eof_count = 0
            done_count = 0

            # read line and call appropriate handlers
            if last_stdout in read_ready:
                line = last_stdout.readline().decode('utf-8').rstrip()
                if line != "":
                    stdout_handler(line)
                else:
                    eof_count = eof_count + 1

            for item in self.items:
                if item.process.stderr in read_ready:
                    line = item.process.stderr.readline().decode('utf-8').rstrip()
                    if line != "":
                        item.stderr_handler(line)
                    else:
                        eof_count = eof_count + 1

                if item.process.poll() is not None:
                    done_count = done_count + 1

            # all filehandles are eof and all processes are done (poll() is not None)
            if eof_count == len(selectors) and done_count == len(self.items):
                break

        # close filehandles
        last_stdout.close()
        for item in self.items:
            item.process.stderr.close()

        # call exit handlers
        success = True
        for item in self.items:
            if item.exit_handler is not None:
                success=item.exit_handler(item.process.returncode) and success

        return success
