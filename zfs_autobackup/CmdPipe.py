import subprocess
import os
import select

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

    def add(self, cmd, readonly=False, stderr_handler=None, exit_handler=None):
        """adds a command to pipe"""

        self.items.append({
            'cmd': cmd,
            'stderr_handler': stderr_handler,
            'exit_handler': exit_handler
        })

        if not readonly and self.readonly:
            self._should_execute = False

    def __str__(self):
        """transform into oneliner for debugging and testing """

        #just one command?
        if len(self.items)==1:
            return " ".join(self.items[0]['cmd'])

        #an actual pipe
        ret = ""
        for item in self.items:
            if ret:
                ret = ret + " | "
            ret = ret + "(" + " ".join(item['cmd']) + ")"

        return ret

    def should_execute(self):
        return(self._should_execute)

    def execute(self, stdout_handler):
        """run the pipe. returns True if it executed, and false if it skipped due to readonly conditions"""

        if not self._should_execute:
            return False

        # first process should have actual user input as stdin:
        selectors = []

        # create processes
        last_stdout = None
        stdin = subprocess.PIPE
        for item in self.items:

            # make sure the command gets all the data in utf8 format:
            # (this is necessary if LC_ALL=en_US.utf8 is not set in the environment)
            encoded_cmd = []
            for arg in item['cmd']:
                encoded_cmd.append(arg.encode('utf-8'))

            item['process'] = subprocess.Popen(encoded_cmd, env=os.environ, stdout=subprocess.PIPE, stdin=stdin,
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

        #close filehandles
        last_stdout.close()
        for item in self.items:
            item['process'].stderr.close()

        #call exit handlers
        for item in self.items:
            if item['exit_handler'] is not None:
                item['exit_handler'](item['process'].returncode)


        return True
