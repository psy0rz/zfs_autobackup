import os
import select
import subprocess

from zfs_autobackup.CmdPipe import CmdPipe
from zfs_autobackup.LogStub import LogStub

class ExecuteError(Exception):
    pass

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

    # def _parse_stderr_pipe(self, line, hide_errors):
    #     """parse stderr from pipe input process. can be overridden in subclass"""
    #     if hide_errors:
    #         self.debug("STDERR|> " + line.rstrip())
    #     else:
    #         self.error("STDERR|> " + line.rstrip())

    def _remote_cmd(self, cmd):
        """transforms cmd in correct form for remote over ssh, if needed"""

        # use ssh?
        if self.ssh_to is not None:
            encoded_cmd = []
            encoded_cmd.append("ssh")

            if self.ssh_config is not None:
                encoded_cmd.extend(["-F", self.ssh_config])

            encoded_cmd.append(self.ssh_to)

            for arg in cmd:
                # add single quotes for remote commands to support spaces and other weird stuff (remote commands are
                # executed in a shell) and escape existing single quotes (bash needs ' to end the quoted string,
                # then a \' for the actual quote and then another ' to start a new quoted string) (and then python
                # needs the double \ to get a single \)
                encoded_cmd.append(("'" + arg.replace("'", "'\\''") + "'"))

            return encoded_cmd
        else:
            return(cmd)


    def is_local(self):
        return self.ssh_to is None


    def run(self, cmd, inp=None, tab_split=False, valid_exitcodes=None, readonly=False, hide_errors=False,
            return_stderr=False, pipe=False):
        """run a command on the node , checks output and parses/handle output and returns it

        :param cmd: the actual command, should be a list, where the first item is the command
                    and the rest are parameters.
        :param pipe: return CmdPipe instead of executing it.
        :param inp: Can be None, a string or a CmdPipe that was previously returned.
        :param tab_split: split tabbed files in output into a list
        :param valid_exitcodes: list of valid exit codes for this command (checks exit code of both sides of a pipe)
                                Use [] to accept all exit codes. Default [0]
        :param readonly: make this True if the command doesn't make any changes and is safe to execute in testmode
        :param hide_errors: don't show stderr output as error, instead show it as debugging output (use to hide expected errors)
        :param return_stderr: return both stdout and stderr as a tuple. (normally only returns stdout)

        """

        # create new pipe?
        if not isinstance(inp, CmdPipe):
            p = CmdPipe(self.readonly, inp)
        else:
            # add stuff to existing pipe
            p = inp

        # stderr parser
        error_lines = []
        def stderr_handler(line):
            if tab_split:
                error_lines.append(line.rstrip().split('\t'))
            else:
                error_lines.append(line.rstrip())
            self._parse_stderr(line, hide_errors)

        # exit code hanlder
        if valid_exitcodes is None:
            valid_exitcodes = [0]

        def exit_handler(exit_code):
            if self.debug_output:
                self.debug("EXIT   > {}".format(exit_code))

            if (valid_exitcodes != []) and (exit_code not in valid_exitcodes):
             raise (ExecuteError("Command '{}' return exit code '{}' (valid codes: {})".format(" ".join(cmd), exit_code, valid_exitcodes)))

        # add command to pipe
        encoded_cmd = self._remote_cmd(cmd)
        p.add(cmd=encoded_cmd, readonly=readonly, stderr_handler=stderr_handler, exit_handler=exit_handler)

        # return pipe instead of executing?
        if pipe:
            return p

        # stdout parser
        output_lines = []
        def stdout_handler(line):
            if tab_split:
                output_lines.append(line.rstrip().split('\t'))
            else:
                output_lines.append(line.rstrip())
            self._parse_stdout(line)

        if p.should_execute():
            self.debug("CMD    > {}".format(p))
        else:
            self.debug("CMDSKIP> {}".format(p))

        # execute and calls handlers in CmdPipe
        p.execute(stdout_handler=stdout_handler)

        if return_stderr:
            return output_lines, error_lines
        else:
            return output_lines
