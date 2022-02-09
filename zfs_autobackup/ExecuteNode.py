import os
import select
import subprocess
from .CmdPipe import CmdPipe, CmdItem
from .LogStub import LogStub

try:
    from shlex import quote as cmd_quote
except ImportError:
    from pipes import quote as cmd_quote


class ExecuteError(Exception):
    pass


class ExecuteNode(LogStub):
    """an endpoint to execute local or remote commands via ssh"""

    PIPE=1

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

    def _quote(self, cmd):
        """return quoted version of command. if it has value PIPE it will add an actual | """
        if cmd==self.PIPE:
            return('|')
        else:
            return cmd_quote(cmd)

    def _shell_cmd(self, cmd, cwd):
        """prefix specified ssh shell to command and escape shell characters"""

        ret=[]

        #add remote shell
        if not self.is_local():
            #note: dont escape this part (executed directly without shell)
            ret=["ssh"]

            if self.ssh_config is not None:
                ret.extend(["-F", self.ssh_config])

            ret.append(self.ssh_to)

        #note: DO escape from here, executed in either local or remote shell.

        shell_str=""

        #add cwd change?
        if cwd is not None:
            shell_str=shell_str + "cd " + self._quote(cwd) + "; "

        shell_str=shell_str + " ".join(map(self._quote, cmd))

        ret.append(shell_str)

        return ret

    def is_local(self):
        return self.ssh_to is None

    def run(self, cmd, inp=None, tab_split=False, valid_exitcodes=None, readonly=False, hide_errors=False,
            return_stderr=False, pipe=False, return_all=False, cwd=None):
        """run a command on the node , checks output and parses/handle output and returns it

        Takes care of proper quoting/escaping/ssh and logging of stdout/err/exit codes.

        Either uses a local shell (sh -c) or remote shell (ssh) to execute the command.
        Therefore the command can have stuff like actual pipes in it, if you dont want to use pipe=True to pipe stuff.

        :param cmd: the actual command, should be a list, where the first item is the command
                    and the rest are parameters. use ExecuteNode.PIPE to add an unescaped |
                    (if you want to use system piping instead of python piping)
        :param pipe: return CmdPipe instead of executing it. (pipe this into another run() command via inp=...)
        :param inp: Can be None, a string or a CmdPipe that was previously returned.
        :param tab_split: split tabbed files in output into a list
        :param valid_exitcodes: list of valid exit codes for this command. Use [] to accept all exit codes. Default [0]
        :param readonly: make this True if the command doesn't make any changes and is safe to execute in testmode
        :param hide_errors: don't show stderr output as error, instead show it as debugging output (use to hide expected errors)
        :param return_stderr: return both stdout and stderr as a tuple. (normally only returns stdout)
        :param return_all: return both stdout and stderr and exit_code as a tuple. (normally only returns stdout)
        :param cwd: Change current working directory before executing command.

        """

        # create new pipe?
        if not isinstance(inp, CmdPipe):
            cmd_pipe = CmdPipe(self.readonly, inp)
        else:
            # add stuff to existing pipe
            cmd_pipe = inp

        # stderr parser
        error_lines = []
        returned_exit_code=None

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
                self.error("Command \"{}\" returned exit code {} (valid codes: {})".format(cmd_item, exit_code, valid_exitcodes))
                return False

            return True

        # stdout parser
        output_lines = []

        if pipe:
            # dont specify output handler, so it will get piped to next process
            stdout_handler=None
        else:
            # handle output manually, dont pipe it
            def stdout_handler(line):
                if tab_split:
                    output_lines.append(line.rstrip().split('\t'))
                else:
                    output_lines.append(line.rstrip())
                self._parse_stdout(line)

        # add shell command and handlers to pipe
        cmd_item=CmdItem(cmd=self._shell_cmd(cmd, cwd), readonly=readonly, stderr_handler=stderr_handler, exit_handler=exit_handler, shell=self.is_local(), stdout_handler=stdout_handler)
        cmd_pipe.add(cmd_item)

        # return CmdPipe instead of executing?
        if pipe:
            return cmd_pipe

        if cmd_pipe.should_execute():
            self.debug("CMD    > {}".format(cmd_pipe))
        else:
            self.debug("CMDSKIP> {}".format(cmd_pipe))

        # execute and calls handlers in CmdPipe
        if not cmd_pipe.execute():
            raise(ExecuteError("Last command returned error"))

        if return_all:
            return output_lines, error_lines, cmd_item.process and cmd_item.process.returncode
        elif return_stderr:
            return output_lines, error_lines
        else:
            return output_lines

    def script(self, lines, inp=None, stdout_handler=None, stderr_handler=None, exit_handler=None, valid_exitcodes=None, readonly=False, hide_errors=False, pipe=False):
        """Run a multiline script on the node.

        This is much more low level than run() and allows for finer grained control.

        Either uses a local shell (sh -c) or remote shell (ssh) to execute the command.
        You need to do your own escaping/quoting.
        It will do logging of stderr and exit codes, but you should
        specify your stdout handler when calling CmdPipe.execute.
        Also specify the optional stderr/exit code handlers if you need them.
        Handlers are called for each line.
        It wont collect lines internally like run() does, so streams of data can be of unlimited size.

        :param lines: list of lines of the actual script.
        :param inp: Can be None, a string or a CmdPipe that was previously returned.
        :param readonly: make this True if the command doesn't make any changes and is safe to execute in testmode
        :param valid_exitcodes: list of valid exit codes for this command. Use [] to accept all exit codes. Default [0]
        :param hide_errors: don't show stderr output as error, instead show it as debugging output (use to hide expected errors)
        :param pipe: return CmdPipe instead of executing it. (pipe this into another run() command via inp=...)

        """

        # create new pipe?
        if not isinstance(inp, CmdPipe):
            cmd_pipe = CmdPipe(self.readonly, inp)
        else:
            # add stuff to existing pipe
            cmd_pipe = inp

        internal_stdout_handler=None
        if stdout_handler is not None:
            if self.debug_output:
                def internal_stdout_handler(line):
                    self.debug("STDOUT > " + line.rstrip())
                    stdout_handler(line)
            else:
                internal_stdout_handler=stdout_handler

        def internal_stderr_handler(line):
            self._parse_stderr(line, hide_errors)
            if stderr_handler is not None:
                stderr_handler(line)

        # exit code hanlder
        if valid_exitcodes is None:
            valid_exitcodes = [0]

        def internal_exit_handler(exit_code):
            if self.debug_output:
                self.debug("EXIT   > {}".format(exit_code))

            if exit_handler is not None:
                exit_handler(exit_code)

            if (valid_exitcodes != []) and (exit_code not in valid_exitcodes):
                self.error("Script returned exit code {} (valid codes: {})".format(exit_code, valid_exitcodes))
                return False

            return True

        #build command
        cmd=[]

        #add remote shell
        if not self.is_local():
            #note: dont escape this part (executed directly without shell)
            cmd.append("ssh")

            if self.ssh_config is not None:
                cmd.append(["-F", self.ssh_config])

            cmd.append(self.ssh_to)

        # convert to script
        cmd.append("\n".join(lines))

        # add shell command and handlers to pipe
        cmd_item=CmdItem(cmd=cmd, readonly=readonly, stderr_handler=internal_stderr_handler, exit_handler=internal_exit_handler, stdout_handler=internal_stdout_handler, shell=self.is_local())
        cmd_pipe.add(cmd_item)

        self.debug("SCRIPT > {}".format(cmd_pipe))

        if pipe:
            return cmd_pipe
        else:
            return cmd_pipe.execute()
