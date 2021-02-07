import os
import select
import subprocess

from zfs_autobackup.LogStub import LogStub


class ExecuteNode(LogStub):
    """an endpoint to execute local or remote commands via ssh"""

    def __init__(self, ssh_config=None, ssh_to=None, readonly=False, debug_output=False):
        """ssh_config: custom ssh config
           ssh_to: server you want to ssh to. none means local
           readonly: only execute commands that don't make any changes (useful for testing-runs)
           debug_output: show output and exit codes of commands in debugging output.
        """

        self.ssh_config = ssh_config
        self.ssh_to = ssh_to
        self.readonly = readonly
        self.debug_output = debug_output

    def __repr__(self):
        if self.ssh_to is None:
            return "(local)"
        else:
            return self.ssh_to

    def _parse_stdout(self, line):
        """parse stdout. can be overridden in subclass"""
        if self.debug_output:
            self.debug("STDOUT > " + line.rstrip())

    def _parse_stderr(self, line, hide_errors):
        """parse stderr. can be overridden in subclass"""
        if hide_errors:
            self.debug("STDERR > " + line.rstrip())
        else:
            self.error("STDERR > " + line.rstrip())

    def _parse_stderr_pipe(self, line, hide_errors):
        """parse stderr from pipe input process. can be overridden in subclass"""
        if hide_errors:
            self.debug("STDERR|> " + line.rstrip())
        else:
            self.error("STDERR|> " + line.rstrip())

    def run(self, cmd, inp=None, tab_split=False, valid_exitcodes=None, readonly=False, hide_errors=False, pipe=False,
            return_stderr=False):
        """run a command on the node.

        :param cmd: the actual command, should be a list, where the first item is the command
                    and the rest are parameters.
        :param inp: Can be None, a string or a pipe-handle you got from another run()
        :param tab_split: split tabbed files in output into a list
        :param valid_exitcodes: list of valid exit codes for this command (checks exit code of both sides of a pipe)
                                Use [] to accept all exit codes.
        :param readonly: make this True if the command doesn't make any changes and is safe to execute in testmode
        :param hide_errors: don't show stderr output as error, instead show it as debugging output (use to hide expected errors)
        :param pipe: Instead of executing, return a pipe-handle to be used to
                     input to another run() command. (just like a | in linux)
        :param return_stderr: return both stdout and stderr as a tuple. (normally only returns stdout)

        """

        if valid_exitcodes is None:
            valid_exitcodes = [0]

        encoded_cmd = []

        # use ssh?
        if self.ssh_to is not None:
            encoded_cmd.append("ssh".encode('utf-8'))

            if self.ssh_config is not None:
                encoded_cmd.extend(["-F".encode('utf-8'), self.ssh_config.encode('utf-8')])

            encoded_cmd.append(self.ssh_to.encode('utf-8'))

            # make sure the command gets all the data in utf8 format:
            # (this is necessary if LC_ALL=en_US.utf8 is not set in the environment)
            for arg in cmd:
                # add single quotes for remote commands to support spaces and other weird stuff (remote commands are
                # executed in a shell) and escape existing single quotes (bash needs ' to end the quoted string,
                # then a \' for the actual quote and then another ' to start a new quoted string) (and then python
                # needs the double \ to get a single \)
                encoded_cmd.append(("'" + arg.replace("'", "'\\''") + "'").encode('utf-8'))

        else:
            for arg in cmd:
                encoded_cmd.append(arg.encode('utf-8'))

        # debug and test stuff
        debug_txt = ""
        for c in encoded_cmd:
            debug_txt = debug_txt + " " + c.decode()

        if pipe:
            debug_txt = debug_txt + " |"

        if self.readonly and not readonly:
            self.debug("SKIP   > " + debug_txt)
        else:
            if pipe:
                self.debug("PIPE   > " + debug_txt)
            else:
                self.debug("RUN    > " + debug_txt)

        # determine stdin
        if inp is None:
            # NOTE: Not None, otherwise it reads stdin from terminal!
            stdin = subprocess.PIPE
        elif isinstance(inp, str) or type(inp) == 'unicode':
            self.debug("INPUT  > \n" + inp.rstrip())
            stdin = subprocess.PIPE
        elif isinstance(inp, subprocess.Popen):
            self.debug("Piping input")
            stdin = inp.stdout
        else:
            raise (Exception("Program error: Incompatible input"))

        if self.readonly and not readonly:
            # todo: what happens if input is piped?
            return

        # execute and parse/return results
        p = subprocess.Popen(encoded_cmd, env=os.environ, stdout=subprocess.PIPE, stdin=stdin, stderr=subprocess.PIPE)

        # Note: make streaming?
        if isinstance(inp, str) or type(inp) == 'unicode':
            p.stdin.write(inp.encode('utf-8'))

        if p.stdin:
            p.stdin.close()

        # return pipe
        if pipe:
            return p

        # handle all outputs
        if isinstance(inp, subprocess.Popen):
            selectors = [p.stdout, p.stderr, inp.stderr]
            inp.stdout.close()  # otherwise inputprocess wont exit when ours does
        else:
            selectors = [p.stdout, p.stderr]

        output_lines = []
        error_lines = []
        while True:
            (read_ready, write_ready, ex_ready) = select.select(selectors, [], [])
            eof_count = 0
            if p.stdout in read_ready:
                line = p.stdout.readline().decode('utf-8')
                if line != "":
                    if tab_split:
                        output_lines.append(line.rstrip().split('\t'))
                    else:
                        output_lines.append(line.rstrip())
                    self._parse_stdout(line)
                else:
                    eof_count = eof_count + 1
            if p.stderr in read_ready:
                line = p.stderr.readline().decode('utf-8')
                if line != "":
                    if tab_split:
                        error_lines.append(line.rstrip().split('\t'))
                    else:
                        error_lines.append(line.rstrip())
                    self._parse_stderr(line, hide_errors)
                else:
                    eof_count = eof_count + 1
            if isinstance(inp, subprocess.Popen) and (inp.stderr in read_ready):
                line = inp.stderr.readline().decode('utf-8')
                if line != "":
                    self._parse_stderr_pipe(line, hide_errors)
                else:
                    eof_count = eof_count + 1

            # stop if both processes are done and all filehandles are EOF:
            if (p.poll() is not None) and (
                    (not isinstance(inp, subprocess.Popen)) or inp.poll() is not None) and eof_count == len(selectors):
                break

        p.stderr.close()
        p.stdout.close()

        if self.debug_output:
            self.debug("EXIT   > {}".format(p.returncode))

        # handle piped process error output and exit codes
        if isinstance(inp, subprocess.Popen):
            inp.stderr.close()
            inp.stdout.close()

            if self.debug_output:
                self.debug("EXIT  |> {}".format(inp.returncode))
            if valid_exitcodes and inp.returncode not in valid_exitcodes:
                raise (subprocess.CalledProcessError(inp.returncode, "(pipe)"))

        if valid_exitcodes and p.returncode not in valid_exitcodes:
            raise (subprocess.CalledProcessError(p.returncode, encoded_cmd))

        if return_stderr:
            return output_lines, error_lines
        else:
            return output_lines
