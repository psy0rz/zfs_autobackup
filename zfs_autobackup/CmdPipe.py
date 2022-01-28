# This is the low level process executing stuff.
# It makes piping and parallel process handling more easy.

# You can specify a handler for each line of stderr output for each item in the pipe.
# Every item also has its own exitcode handler.

# Normally you add a stdout_handler to the last item in the pipe.
# However: You can also add stdout_handler to other items in a pipe. This will turn that item in to a manual pipe: your
# handler is responsible for sending data into the next item of the pipe. (avaiable in item.next)

# You can also use manual pipe mode to just execute multiple command in parallel and handle their output parallel,
# without doing any actual pipe stuff. (because you dont HAVE to send data into the next item.)


import subprocess
import os
import select

try:
    from shlex import quote as cmd_quote
except ImportError:
    from pipes import quote as cmd_quote


class CmdItem:
    """one command item, to be added to a CmdPipe"""

    def __init__(self, cmd, readonly=False, stderr_handler=None, exit_handler=None, stdout_handler=None, shell=False):
        """create item. caller has to make sure cmd is properly escaped when using shell.

        If stdout_handler is None, it will connect the stdout to the stdin of the next item in the pipe, like
        and actual system pipe. (no python overhead)

        :type cmd: list of str
        """

        self.cmd = cmd
        self.readonly = readonly
        self.stderr_handler = stderr_handler
        self.stdout_handler = stdout_handler
        self.exit_handler = exit_handler
        self.shell = shell
        self.process = None
        self.next = None #next item in pipe, set by CmdPipe

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

    def execute(self):
        """run the pipe. returns True all exit handlers returned true. (otherwise it will be False/None depending on exit handlers returncode) """

        if not self._should_execute:
            return True

        selectors = self.__create()

        if not selectors:
            raise (Exception("Cant use cmdpipe without any output handlers."))

        self.__process_outputs(selectors)

        # close filehandles
        for item in self.items:
            item.process.stderr.close()
            item.process.stdout.close()

        # call exit handlers
        success = True
        for item in self.items:
            if item.exit_handler is not None:
                success=item.exit_handler(item.process.returncode) and success

        return success

    def __process_outputs(self, selectors):
        """watch all output selectors and call handlers"""

        while True:
            # wait for output on one of the stderrs or last_stdout
            (read_ready, write_ready, ex_ready) = select.select(selectors, [], [])

            eof_count = 0
            done_count = 0

            # read line and call appropriate handlers

            for item in self.items:
                if item.process.stdout in read_ready:
                    line = item.process.stdout.readline().decode('utf-8').rstrip()
                    if line != "":
                        item.stdout_handler(line)
                    else:
                        eof_count = eof_count + 1
                        if item.next:
                            item.next.process.stdin.close()

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



    def __create(self):
        """create actual processes, do piping and return selectors."""

        selectors = []
        next_stdin = subprocess.PIPE  # means we write input via python instead of an actual system pipe
        first = True
        prev_item = None

        for item in self.items:

            # creates the actual subprocess via subprocess.popen
            item.create(next_stdin)

            # we piped previous process? dont forget to close its stdout
            if next_stdin != subprocess.PIPE:
                next_stdin.close()

            if item.stderr_handler:
                selectors.append(item.process.stderr)

            # we're the first process in the pipe
            if first:
                if self.inp is not None:
                    # write the input we have
                    item.process.stdin.write(self.inp.encode('utf-8'))
                item.process.stdin.close()
                first = False

            # manual stdout handling or pipe it to the next process?
            if item.stdout_handler is None:
                # no manual stdout handling, pipe it to the next process via sytem pipe
                next_stdin = item.process.stdout
            else:
                # manual stdout handling via python
                selectors.append(item.process.stdout)
                # next process will get input from python:
                next_stdin = subprocess.PIPE

            if prev_item is not None:
                prev_item.next = item

            prev_item = item
        return selectors
