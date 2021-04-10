import subprocess
import os
import select

class CmdPipe:
    """a pipe of one or more commands """

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

    def add(self, cmd, readonly=False, stderr_handler=None):
        """adds a command to pipe"""

        self.items.append({
            'cmd': cmd,
            'stderr_handler': stderr_handler
        })

        if not readonly and self.readonly:
            self._should_execute = False

    def __str__(self):
        """transform into oneliner for debugging and testing """
        ret = ""
        for item in self.items:
            if ret:
                ret = ret + " | "
            ret = ret + "(" + " ".join(item['cmd']) + ")"

        return ret

    def execute(self, stdout_handler):
        """run the pipe"""

        if not self._should_execute:
            return None

        # first process should have actual user input as stdin:
        selectors = []

        # create processes
        last_stdout = None
        stdin = subprocess.PIPE
        for item in self.items:
            item['process'] = subprocess.Popen(item['cmd'], env=os.environ, stdout=subprocess.PIPE, stdin=stdin,
                                               stderr=subprocess.PIPE)

            selectors.append(item['process'].stderr)

            if last_stdout is None:
                # we're the first process in the pipe, do we have some input?
                if self.inp is not None:
                    # TODO: make streaming to support big inputs?
                    item['process'].stdin.write(self.inp.encode('utf-8'))
                item['process'].stdin.close()
            else:
                #last stdout was piped to this stdin already, so close it because we dont need it anymore
                last_stdout.close()

            last_stdout = item['process'].stdout
            stdin=last_stdout

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
                if item['process'].stderr in read_ready:
                    line = item['process'].stderr.readline().decode('utf-8').rstrip()
                    if line != "":
                        item['stderr_handler'](line)
                    else:
                        eof_count = eof_count + 1

                if item['process'].poll() is not None:
                    done_count = done_count + 1

            # all filehandles are eof and all processes are done (poll() is not None)
            if eof_count == len(selectors) and done_count == len(self.items):
                break

        # close all filehandles and get all exit codes
        ret = []
        last_stdout.close()
        for item in self.items:
            item['process'].stderr.close()
            ret.append(item['process'].returncode)

        return ret
