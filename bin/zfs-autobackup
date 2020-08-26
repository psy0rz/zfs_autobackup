#!/usr/bin/env python
# -*- coding: utf8 -*-

# (c)edwin@datux.nl  - Released under GPL V3
#
# Greetings from eth0 2019 :)

from __future__ import print_function

import os
import sys
import re
import traceback
import subprocess
import pprint
import time
import argparse
from pprint import pprint as p
import select

use_color=False
if sys.stdout.isatty():
    try:
        import colorama
        use_color=True
    except ImportError:
        pass

VERSION="3.0"
HEADER="zfs-autobackup v{} - Copyright 2020 E.H.Eefting (edwin@datux.nl)".format(VERSION)

class Log:
    def __init__(self, show_debug=False, show_verbose=False):
        self.last_log=""
        self.show_debug=show_debug
        self.show_verbose=show_verbose

    def error(self, txt):
        if use_color:
            print(colorama.Fore.RED+colorama.Style.BRIGHT+ "! "+txt+colorama.Style.RESET_ALL, file=sys.stderr)
        else:
            print("! "+txt, file=sys.stderr)
        sys.stderr.flush()

    def verbose(self, txt):
        if self.show_verbose:
            if use_color:
                print(colorama.Style.NORMAL+ "  "+txt+colorama.Style.RESET_ALL)
            else:
                print("  "+txt)
            sys.stdout.flush()

    def debug(self, txt):
        if self.show_debug:
            if use_color:
                print(colorama.Fore.GREEN+ "# "+txt+colorama.Style.RESET_ALL)
            else:
                print("# "+txt)
            sys.stdout.flush()





class ThinnerRule:
    """a thinning schedule rule for Thinner"""

    TIME_NAMES={
            'y'   : 3600 * 24 * 365.25,
            'm'   : 3600 * 24 * 30,
            'w'   : 3600 * 24 * 7,
            'd'   : 3600 * 24,
            'h'   : 3600,
            'min' : 60,
            's'   : 1,
    }

    TIME_DESC={
            'y'   : 'year',
            'm'   : 'month',
            'w'   : 'week',
            'd'   : 'day',
            'h'   : 'hour',
            'min' : 'minute',
            's'   : 'second',
    }

    def parse_rule(self, rule_str):
        """parse scheduling string
            example:
                daily snapshot, remove after a week:     1d1w
                weekly snapshot, remove after a month:   1w1m
                monthly snapshot, remove after 6 months: 1m6m
                yearly snapshot, remove after 2 year:    1y2y
                keep all snapshots, remove after a day   1s1d
                keep nothing:                            1s1s

        """

        rule_str=rule_str.lower()
        matches=re.findall("([0-9]*)([a-z]*)([0-9]*)([a-z]*)", rule_str)[0]

        period_amount=int(matches[0])
        period_unit=matches[1]
        ttl_amount=int(matches[2])
        ttl_unit=matches[3]

        if not period_unit in self.TIME_NAMES:
            raise(Exception("Invalid period string in schedule: '{}'".format(rule_str)))

        if not ttl_unit in self.TIME_NAMES:
            raise(Exception("Invalid ttl string in schedule: '{}'".format(rule_str)))


        self.period=period_amount * self.TIME_NAMES[period_unit]
        self.ttl=ttl_amount * self.TIME_NAMES[ttl_unit]

        if self.period>self.ttl:
            raise(Exception("Period cant be longer than ttl in schedule: '{}'".format(rule_str)))

        self.rule_str=rule_str

        self.human_str="Keep every {} {}{}, delete after {} {}{}.".format(
            period_amount, self.TIME_DESC[period_unit], period_amount!=1 and "s" or "", ttl_amount, self.TIME_DESC[ttl_unit], ttl_amount!=1 and "s" or "" )


    def __str__(self):
        """get schedule as a schedule string"""

        return(self.rule_str)




    def __init__(self, rule_str):
        self.parse_rule(rule_str)
        pass


class Thinner:
    """progressive thinner (universal, used for cleaning up snapshots)"""

    def __init__(self, schedule_str=""):
        """schedule_str: comma seperated list of ThinnerRules. A plain number specifies how many snapshots to always keep.
        """

        self.rules=[]
        self.always_keep=0

        if schedule_str=="":
            return

        rule_strs=schedule_str.split(",")
        for rule_str in rule_strs:
            if rule_str.isdigit():
                self.always_keep=int(rule_str)
                if self.always_keep<0:
                    raise(Exception("Number of snapshots to keep cant be negative: {}".format(self.keep_source)))
            else:
                self.rules.append(ThinnerRule(rule_str))

    def human_rules(self):
        """get list of human readable rules"""
        ret=[]
        if self.always_keep:
            ret.append("Keep the last {} snapshot{}.".format(self.always_keep, self.always_keep!=1 and "s" or ""))
        for rule in self.rules:
            ret.append(rule.human_str)

        return(ret)

    def thin(self,objects, keep_objects=[], now=None):
        """thin list of objects with current schedule rules.
        objects: list of objects to thin. every object should have timestamp attribute.
        keep_objects: objects to always keep (these should also be in normal objects list, so we can use them to perhaps delete other obsolete objects)

            return( keeps, removes )
        """

        #always keep a number of the last objets?
        if self.always_keep:
            #all of them
            if len(objects)<=self.always_keep:
                return ( (objects, []) )

            #determine which ones
            always_keep_objects=objects[-self.always_keep:]
        else:
            always_keep_objects=[]


        #determine time blocks
        time_blocks={}
        for rule in self.rules:
            time_blocks[rule.period]={}

        if not now:
            now=int(time.time())

        keeps=[]
        removes=[]

        #traverse objects
        for object in objects:
            #important they are ints!
            timestamp=int(object.timestamp)
            age=int(now)-timestamp

            # store in the correct time blocks, per period-size, if not too old yet
            # e.g.: look if there is ANY timeblock that wants to keep this object
            keep=False
            for rule in self.rules:
                if age<=rule.ttl:
                    block_nr=int(timestamp/rule.period)
                    if not block_nr in time_blocks[rule.period]:
                        time_blocks[rule.period][block_nr]=True
                        keep=True

            #keep it according to schedule, or keep it because it is in the keep_objects list
            if keep or object in keep_objects or object in always_keep_objects:
                keeps.append(object)
            else:
                removes.append(object)

        return( (keeps, removes) )



class cached_property(object):
    """ A property that is only computed once per instance and then replaces
        itself with an ordinary attribute. Deleting the attribute resets the
        property.

        Source: https://github.com/bottlepy/bottle/commit/fa7733e075da0d790d809aa3d2f53071897e6f76
        """

    def __init__(self, func):
        self.__doc__ = getattr(func, '__doc__')
        self.func = func


    def __get__(self, obj, cls):
        if obj is None:
            return self

        propname=self.func.__name__

        if not hasattr(obj, '_cached_properties'):
            obj._cached_properties={}

        if not propname in obj._cached_properties:
            obj._cached_properties[propname]=self.func(obj)
            # value = obj.__dict__[propname] = self.func(obj)

        return obj._cached_properties[propname]

class Logger():

    #simple logging stubs
    def debug(self, txt):
        print("DEBUG  : "+txt)

    def verbose(self, txt):
        print("VERBOSE: "+txt)

    def error(self, txt):
        print("ERROR  : "+txt)



class ExecuteNode(Logger):
    """an endpoint to execute local or remote commands via ssh"""


    def __init__(self, ssh_config=None, ssh_to=None, readonly=False, debug_output=False):
        """ssh_config: custom ssh config
           ssh_to: server you want to ssh to. none means local
           readonly: only execute commands that don't make any changes (useful for testing-runs)
           debug_output: show output and exit codes of commands in debugging output.
        """

        self.ssh_config=ssh_config
        self.ssh_to=ssh_to
        self.readonly=readonly
        self.debug_output=debug_output

    def __repr__(self):
        if self.ssh_to==None:
            return("(local)")
        else:
            return(self.ssh_to)

    def _parse_stdout(self, line):
        """parse stdout. can be overridden in subclass"""
        if self.debug_output:
            self.debug("STDOUT > "+line.rstrip())


    def _parse_stderr(self, line, hide_errors):
        """parse stderr. can be overridden in subclass"""
        if  hide_errors:
            self.debug("STDERR > "+line.rstrip())
        else:
            self.error("STDERR > "+line.rstrip())

    def _parse_stderr_pipe(self, line, hide_errors):
        """parse stderr from pipe input process. can be overridden in subclass"""
        if hide_errors:
            self.debug("STDERR|> "+line.rstrip())
        else:
            self.error("STDERR|> "+line.rstrip())


    def run(self, cmd, input=None, tab_split=False, valid_exitcodes=[ 0 ], readonly=False, hide_errors=False, pipe=False, return_stderr=False):
        """run a command on the node
        cmd: the actual command, should be a list, where the first item is the command and the rest are parameters.
        input: Can be None, a string or a pipe-handle you got from another run()
        tab_split: split tabbed files in output into a list
        valid_exitcodes: list of valid exit codes for this command (checks exit code of both sides of a pipe)
        readonly: make this True if the command doesn't make any changes and is safe to execute in testmode
        hide_errors: don't show stderr output as error, instead show it as debugging output (use to hide expected errors)
        pipe: Instead of executing, return a pipe-handle to be used to input to another run() command. (just like a | in linux)
        return_stderr: return both stdout and stderr as a tuple. (only returns stderr from this side of the pipe)
        """

        encoded_cmd=[]

        #use ssh?
        if self.ssh_to != None:
            encoded_cmd.append("ssh".encode('utf-8'))

            if self.ssh_config != None:
                encoded_cmd.extend(["-F".encode('utf-8'), self.ssh_config.encode('utf-8')])

            encoded_cmd.append(self.ssh_to.encode('utf-8'))

            #make sure the command gets all the data in utf8 format:
            #(this is necessary if LC_ALL=en_US.utf8 is not set in the environment)
            for arg in cmd:
                #add single quotes for remote commands to support spaces and other weird stuff (remote commands are executed in a shell)
                #and escape existing single quotes (bash needs ' to end the quoted string, then a \' for the actual quote and then another ' to start a new quoted string)
                #(and then python needs the double \ to get a single \)
                encoded_cmd.append( ("'" + arg.replace("'","'\\''") + "'").encode('utf-8'))

        else:
            for arg in cmd:
                encoded_cmd.append(arg.encode('utf-8'))

        #debug and test stuff
        debug_txt=""
        for c in encoded_cmd:
            debug_txt=debug_txt+" "+c.decode()

        if pipe:
            debug_txt=debug_txt+" |"

        if self.readonly and not readonly:
            self.debug("SKIP   > "+ debug_txt)
        else:
            if pipe:
                self.debug("PIPE   > "+ debug_txt)
            else:
                self.debug("RUN    > "+ debug_txt)

        #determine stdin
        if input==None:
            #NOTE: Not None, otherwise it reads stdin from terminal!
            stdin=subprocess.PIPE
        elif isinstance(input,str) or type(input)=='unicode':
            self.debug("INPUT  > \n"+input.rstrip())
            stdin=subprocess.PIPE
        elif isinstance(input, subprocess.Popen):
            self.debug("Piping input")
            stdin=input.stdout
        else:
            raise(Exception("Program error: Incompatible input"))

        if self.readonly and not readonly:
            #todo: what happens if input is piped?
            return

        #execute and parse/return results
        p=subprocess.Popen(encoded_cmd, env=os.environ, stdout=subprocess.PIPE, stdin=stdin, stderr=subprocess.PIPE)

        #Note: make streaming?
        if isinstance(input,str) or type(input)=='unicode':
            p.stdin.write(input.encode('utf-8'))

        if p.stdin:
            p.stdin.close()

        #return pipe
        if pipe:
            return(p)

        #handle all outputs
        if isinstance(input, subprocess.Popen):
            selectors=[p.stdout, p.stderr, input.stderr ]
            input.stdout.close() #otherwise inputprocess wont exit when ours does
        else:
            selectors=[p.stdout, p.stderr ]

        output_lines=[]
        error_lines=[]
        while True:
            (read_ready, write_ready, ex_ready)=select.select(selectors, [], [])
            eof_count=0
            if p.stdout in read_ready:
                line=p.stdout.readline().decode('utf-8')
                if line!="":
                    if tab_split:
                        output_lines.append(line.rstrip().split('\t'))
                    else:
                        output_lines.append(line.rstrip())
                    self._parse_stdout(line)
                else:
                    eof_count=eof_count+1
            if p.stderr in read_ready:
                line=p.stderr.readline().decode('utf-8')
                if line!="":
                    if tab_split:
                        error_lines.append(line.rstrip().split('\t'))
                    else:
                        error_lines.append(line.rstrip())
                    self._parse_stderr(line, hide_errors)
                else:
                    eof_count=eof_count+1
            if isinstance(input, subprocess.Popen) and (input.stderr in read_ready):
                line=input.stderr.readline().decode('utf-8')
                if line!="":
                    self._parse_stderr_pipe(line, hide_errors)
                else:
                    eof_count=eof_count+1

            #stop if both processes are done and all filehandles are EOF:
            if p.poll()!=None and ((not isinstance(input, subprocess.Popen)) or input.poll()!=None) and eof_count==len(selectors):
                break

        p.stderr.close()
        p.stdout.close()

        if self.debug_output:
            self.debug("EXIT   > {}".format(p.returncode))

        #handle piped process error output and exit codes
        if isinstance(input, subprocess.Popen):
            input.stderr.close()
            input.stdout.close()

            if self.debug_output:
                self.debug("EXIT  |> {}".format(input.returncode))
            if valid_exitcodes and input.returncode not in valid_exitcodes:
                raise(subprocess.CalledProcessError(input.returncode, "(pipe)"))

        if valid_exitcodes and p.returncode not in valid_exitcodes:
            raise(subprocess.CalledProcessError(p.returncode, encoded_cmd))


        if return_stderr:
            return ( output_lines, error_lines )
        else:
            return(output_lines)


class ZfsPool():
    """a zfs pool"""

    def __init__(self, zfs_node, name):
        """name: name of the pool
        """

        self.zfs_node=zfs_node
        self.name=name 

    def __repr__(self):
        return("{}: {}".format(self.zfs_node, self.name))

    def __str__(self):
        return(self.name)

    def __eq__(self, obj):
        if not isinstance(obj, ZfsPool):
            return(False)

        return(self.name == obj.name)

    def verbose(self,txt):
        self.zfs_node.verbose("zpool {}: {}".format(self.name, txt))

    def error(self,txt):
        self.zfs_node.error("zpool {}: {}".format(self.name, txt))

    def debug(self,txt):
        self.zfs_node.debug("zpool {}: {}".format(self.name, txt))


    @cached_property
    def properties(self):
        """all zpool properties"""

        self.debug("Getting zpool properties")

        cmd=[
            "zpool", "get", "-H", "-p", "all", self.name
        ]


        ret={}

        for pair in self.zfs_node.run(tab_split=True, cmd=cmd, readonly=True, valid_exitcodes=[ 0 ]):
            if len(pair)==4:
                ret[pair[1]]=pair[2]

        return(ret)

    @property
    def features(self):
        """get list of active zpool features"""

        ret=[]
        for (key,value) in self.properties.items():
            if key.startswith("feature@"):
                feature=key.split("@")[1]
                if value=='enabled' or value=='active':
                    ret.append(feature)

        return(ret)






class ZfsDataset():
    """a zfs dataset (filesystem/volume/snapshot/clone)
    Note that a dataset doesn't have to actually exist (yet/anymore)
    Also most properties are cached for performance-reasons, but also to allow --test to function correctly.

    """

    # illegal properties per dataset type. these will be removed from --set-properties and --filter-properties
    ILLEGAL_PROPERTIES={
        'filesystem': [ ],
        'volume': [ "canmount" ],
    }


    def __init__(self, zfs_node, name, force_exists=None):
        """name: full path of the zfs dataset
        exists: specify if you already know a dataset exists or not. for performance and testing reasons. (otherwise it will have to check with zfs list when needed)
        """
        self.zfs_node=zfs_node
        self.name=name #full name
        self.force_exists=force_exists

    def __repr__(self):
        return("{}: {}".format(self.zfs_node, self.name))

    def __str__(self):
        return(self.name)

    def __eq__(self, obj):
        if not isinstance(obj, ZfsDataset):
            return(False)

        return(self.name == obj.name)

    def verbose(self,txt):
        self.zfs_node.verbose("{}: {}".format(self.name, txt))

    def error(self,txt):
        self.zfs_node.error("{}: {}".format(self.name, txt))

    def debug(self,txt):
        self.zfs_node.debug("{}: {}".format(self.name, txt))


    def invalidate(self):
        """clear cache"""
        #TODO: nicer?
        self._cached_properties={}
        self.force_exists=None


    def split_path(self):
        """return the path elements as an array"""
        return(self.name.split("/"))

    def lstrip_path(self,count):
        """return name with first count components stripped"""
        return("/".join(self.split_path()[count:]))


    def rstrip_path(self,count):
        """return name with last count components stripped"""
        return("/".join(self.split_path()[:-count]))


    @property
    def filesystem_name(self):
        """filesystem part of the name (before the @)"""
        if self.is_snapshot:
            ( filesystem, snapshot )=self.name.split("@")
            return(filesystem)
        else:
            return(self.name)


    @property
    def snapshot_name(self):
        """snapshot part of the name"""
        if not self.is_snapshot:
            raise(Exception("This is not a snapshot"))

        (filesystem, snapshot_name)=self.name.split("@")
        return(snapshot_name)


    @property
    def is_snapshot(self):
        """true if this dataset is a snapshot"""
        return(self.name.find("@")!=-1)


    @cached_property
    def parent(self):
        """get zfs-parent of this dataset.
        for snapshots this means it will get the filesystem/volume that it belongs to. otherwise it will return the parent according to path

        we cache this so everything in the parent that is cached also stays.
        """
        if self.is_snapshot:
            return(ZfsDataset(self.zfs_node, self.filesystem_name))
        else:
            return(ZfsDataset(self.zfs_node, self.rstrip_path(1)))


    def find_prev_snapshot(self, snapshot, other_snapshots=False):
        """find previous snapshot in this dataset. None if it doesn't exist.

        other_snapshots: set to true to also return snapshots that where not created by us. (is_ours)
        """

        if self.is_snapshot:
            raise(Exception("Please call this on a dataset."))

        index=self.find_snapshot_index(snapshot)
        while index:
            index=index-1
            if other_snapshots or self.snapshots[index].is_ours():
                return(self.snapshots[index])
        return(None)


    def find_next_snapshot(self, snapshot, other_snapshots=False):
        """find next snapshot in this dataset. None if it doesn't exist"""

        if self.is_snapshot:
            raise(Exception("Please call this on a dataset."))

        index=self.find_snapshot_index(snapshot)
        while index!=None and index<len(self.snapshots)-1:
            index=index+1
            if other_snapshots or self.snapshots[index].is_ours():
                return(self.snapshots[index])
        return(None)


    @cached_property
    def exists(self):
        """check if dataset exists.
        Use force to force a specific value to be cached, if you already know. Useful for performance reasons"""


        if self.force_exists!=None:
            self.debug("Checking if filesystem exists: forced to {}".format(self.force_exists))
            return(self.force_exists)
        else:
            self.debug("Checking if filesystem exists")


        return(self.zfs_node.run(tab_split=True, cmd=[ "zfs", "list", self.name], readonly=True, valid_exitcodes=[ 0,1 ], hide_errors=True) and True)


    def create_filesystem(self, parents=False):
        """create a filesystem"""
        if parents:
            self.verbose("Creating filesystem and parents")
            self.zfs_node.run(["zfs", "create", "-p", self.name ])
        else:
            self.verbose("Creating filesystem")
            self.zfs_node.run(["zfs", "create", self.name ])

        #update cache
        self.exists=1


    def destroy(self, fail_exception=False):
        """destroy the dataset. by default failures are not an exception, so we can continue making backups"""

        self.verbose("Destroying")

        if self.is_snapshot:
            self.release()

        try:
            self.zfs_node.run(["zfs", "destroy", self.name])
            self.invalidate()
            self.force_exists=False
            return(True)
        except:
            if not fail_exception:
                return(False)
            else:
                raise


    @cached_property
    def properties(self):
        """all zfs properties"""
        self.debug("Getting zfs properties")

        cmd=[
            "zfs", "get", "-H", "-o", "property,value", "-p", "all", self.name
        ]

        if not self.exists:
            return({})


        ret={}

        for pair in self.zfs_node.run(tab_split=True, cmd=cmd, readonly=True, valid_exitcodes=[ 0 ]):
            if len(pair)==2:
                ret[pair[0]]=pair[1]

        return(ret)


    def is_changed(self, min_changed_bytes=1):
        """dataset is changed since ANY latest snapshot ?"""
        self.debug("Checking if dataset is changed")

        if min_changed_bytes==0:
            return(True)

        if int(self.properties['written'])<min_changed_bytes:
            return(False)
        else:
            return(True)


    def is_ours(self):
        """return true if this snapshot is created by this backup_name"""
        if re.match("^"+self.zfs_node.backup_name+"-[0-9]*$", self.snapshot_name):
            return(True)
        else:
            return(False)


    @property
    def _hold_name(self):
        return("zfs_autobackup:"+self.zfs_node.backup_name)


    @property
    def holds(self):
        """get list of holds for dataset"""

        output=self.zfs_node.run([ "zfs" , "holds", "-H", self.name ], valid_exitcodes=[ 0 ], tab_split=True, readonly=True)
        return(map(lambda fields: fields[1], output))


    def is_hold(self):
        """did we hold this snapshot?"""
        return(self._hold_name in self.holds)


    def hold(self):
        """hold dataset"""
        self.debug("holding")
        self.zfs_node.run([ "zfs" , "hold", self._hold_name, self.name ], valid_exitcodes=[ 0,1 ])


    def release(self):
        """release dataset"""
        if self.zfs_node.readonly or self.is_hold():
            self.debug("releasing")
            self.zfs_node.run([ "zfs" , "release", self._hold_name, self.name ], valid_exitcodes=[ 0,1 ])


    @property
    def timestamp(self):
        """get timestamp from snapshot name. Only works for our own snapshots with the correct format."""
        time_str=re.findall("^.*-([0-9]*)$", self.snapshot_name)[0]
        if len(time_str)!=14:
            raise(Exception("Snapshot has invalid timestamp in name: {}".format(self.snapshot_name)))

        #new format:
        time_secs=time.mktime(time.strptime(time_str,"%Y%m%d%H%M%S"))
        return(time_secs)


    def from_names(self, names):
        """convert a list of names to a list ZfsDatasets for this zfs_node"""
        ret=[]
        for name in names:
            ret.append(ZfsDataset(self.zfs_node, name))

        return(ret)

    @cached_property
    def snapshots(self):
        """get all snapshots of this dataset"""
        self.debug("Getting snapshots")

        if not self.exists:
            return([])

        cmd=[
            "zfs", "list", "-d", "1", "-r", "-t" ,"snapshot", "-H", "-o", "name", self.name
        ]

        names=self.zfs_node.run(cmd=cmd, readonly=True)
        return(self.from_names(names))

    @property
    def our_snapshots(self):
        """get list of snapshots creates by us of this dataset"""
        ret=[]
        for snapshot in self.snapshots:
            if snapshot.is_ours():
                ret.append(snapshot)

        return(ret)


    def find_snapshot(self, snapshot):
        """find snapshot by snapshot (can be a snapshot_name or a different ZfsDataset )"""

        if not isinstance(snapshot,ZfsDataset):
            snapshot_name=snapshot
        else:
            snapshot_name=snapshot.snapshot_name

        for snapshot in self.snapshots:
            if snapshot.snapshot_name==snapshot_name:
                return(snapshot)

        return(None)


    def find_snapshot_index(self, snapshot):
        """find snapshot index by snapshot (can be a snapshot_name or ZfsDataset)"""

        if not isinstance(snapshot,ZfsDataset):
            snapshot_name=snapshot
        else:
            snapshot_name=snapshot.snapshot_name

        index=0
        for snapshot in self.snapshots:
            if snapshot.snapshot_name==snapshot_name:
                return(index)
            index=index+1

        return(None)


    @cached_property
    def written_since_ours(self):
        """get number of bytes written since our last snapshot"""
        self.debug("Getting bytes written since our last snapshot")

        latest_snapshot=self.our_snapshots[-1]

        cmd=[ "zfs", "get","-H" ,"-ovalue", "-p", "written@"+str(latest_snapshot), self.name ]

        output=self.zfs_node.run(readonly=True, tab_split=False, cmd=cmd, valid_exitcodes=[ 0 ])

        return(int(output[0]))


    def is_changed_ours(self, min_changed_bytes=1):
        """dataset is changed since OUR latest snapshot?"""

        if min_changed_bytes==0:
            return(True)

        if not self.our_snapshots:
            return(True)

        #NOTE: filesystems can have a very small amount written without actual changes in some cases
        if self.written_since_ours<min_changed_bytes:
            return(False)

        return(True)


    @cached_property
    def recursive_datasets(self, types="filesystem,volume"):
        """get all (non-snapshot) datasets recursively under us"""

        self.debug("Getting all recursive datasets under us")

        names=self.zfs_node.run(tab_split=False, readonly=True, valid_exitcodes=[ 0 ], cmd=[
            "zfs", "list", "-r", "-t",  types, "-o", "name", "-H", self.name
        ])

        return(self.from_names(names[1:]))


    @cached_property
    def datasets(self, types="filesystem,volume"):
        """get all (non-snapshot) datasets directly under us"""

        self.debug("Getting all datasets under us")

        names=self.zfs_node.run(tab_split=False, readonly=True, valid_exitcodes=[ 0 ], cmd=[
            "zfs", "list", "-r", "-t",  types, "-o", "name", "-H", "-d", "1", self.name
        ])

        return(self.from_names(names[1:]))


    def send_pipe(self, features, prev_snapshot=None, resume_token=None, show_progress=False, raw=False):
        """returns a pipe with zfs send output for this snapshot

        resume_token: resume sending from this token. (in that case we don't need to know snapshot names)

        """
        #### build source command
        cmd=[]

        cmd.extend(["zfs", "send",  ])

        #all kind of performance options:
        if 'large_blocks' in features and "-L" in self.zfs_node.supported_send_options:
            cmd.append("-L") # large block support (only if recordsize>128k which is seldomly used)

        if 'embedded_data' in features and "-e" in self.zfs_node.supported_send_options:
            cmd.append("-e") # WRITE_EMBEDDED, more compact stream

        if "-c" in self.zfs_node.supported_send_options:
            cmd.append("-c") # use compressed WRITE records

        #NOTE: performance is usually worse with this option, according to manual
        #also -D will be depricated in newer ZFS versions
        # if not resume:
        #     if "-D" in self.zfs_node.supported_send_options:
        #         cmd.append("-D") # dedupped stream, sends less duplicate data

        #raw? (for encryption)
        if raw:
            cmd.append("--raw")


        #progress output
        if show_progress:
            cmd.append("-v")
            cmd.append("-P")


        #resume a previous send? (don't need more parameters in that case)
        if resume_token:
            cmd.extend([ "-t", resume_token ])

        else:
            #send properties
            cmd.append("-p")

            #incremental?
            if prev_snapshot:
                cmd.extend([ "-i", prev_snapshot.snapshot_name ])

            cmd.append(self.name)


        # if args.buffer and args.ssh_source!="local":
        #     cmd.append("|mbuffer -m {}".format(args.buffer))

        #NOTE: this doesn't start the send yet, it only returns a subprocess.Pipe
        return(self.zfs_node.run(cmd, pipe=True))


    def recv_pipe(self, pipe, features, filter_properties=[], set_properties=[], ignore_exit_code=False):
        """starts a zfs recv for this snapshot and uses pipe as input

        note: you can it both on a snapshot or filesystem object.
        The resulting zfs command is the same, only our object cache is invalidated differently.
        """
        #### build target command
        cmd=[]

        cmd.extend(["zfs", "recv"])

        #don't mount filesystem that is received
        cmd.append("-u")

        for property in filter_properties:
            cmd.extend([ "-x" , property ])

        for property in set_properties:
            cmd.extend([ "-o" , property ])

        #verbose output
        cmd.append("-v")

        if 'extensible_dataset' in features and "-s" in self.zfs_node.supported_recv_options:
            #support resuming
            self.debug("Enabled resume support")
            cmd.append("-s")

        cmd.append(self.filesystem_name)

        if ignore_exit_code:
            valid_exitcodes=[]
        else:
            valid_exitcodes=[0]

        self.zfs_node.reset_progress()
        self.zfs_node.run(cmd, input=pipe, valid_exitcodes=valid_exitcodes)

        #invalidate cache, but we at least know we exist now
        self.invalidate()

        #in test mode we assume everything was ok and it exists
        if self.zfs_node.readonly:
            self.force_exists=True

        #check if transfer was really ok (exit codes have been wrong before due to bugs in zfs-utils and can be ignored by some parameters)
        if not self.exists:
            self.error("error during transfer")
            raise(Exception("Target doesn't exist after transfer, something went wrong."))

        # if args.buffer and  args.ssh_target!="local":
        #     cmd.append("|mbuffer -m {}".format(args.buffer))


    def transfer_snapshot(self, target_snapshot, features, prev_snapshot=None, show_progress=False, filter_properties=[], set_properties=[], ignore_recv_exit_code=False, resume_token=None, raw=False):
        """transfer this snapshot to target_snapshot. specify prev_snapshot for incremental transfer

        connects a send_pipe() to recv_pipe()
        """

        self.debug("Transfer snapshot to {}".format(target_snapshot.filesystem_name))

        if resume_token:
            target_snapshot.verbose("resuming")

        #initial or increment
        if not prev_snapshot:
            target_snapshot.verbose("receiving full".format(self.snapshot_name))
        else:
            #incremental
            target_snapshot.verbose("receiving incremental".format(self.snapshot_name))

        #do it
        pipe=self.send_pipe(features=features, show_progress=show_progress, prev_snapshot=prev_snapshot, resume_token=resume_token, raw=raw)
        target_snapshot.recv_pipe(pipe, features=features, filter_properties=filter_properties, set_properties=set_properties, ignore_exit_code=ignore_recv_exit_code)

    def abort_resume(self):
        """abort current resume state"""
        self.zfs_node.run(["zfs", "recv", "-A", self.name])


    def rollback(self):
        """rollback to latest existing snapshot on this dataset"""
        self.debug("Rolling back")

        for snapshot in reversed(self.snapshots):
            if snapshot.exists:
                self.zfs_node.run(["zfs", "rollback", snapshot.name])
                return


    def get_resume_snapshot(self, resume_token):
        """returns snapshot that will be resumed by this resume token (run this on source with target-token)"""
        #use zfs send -n option to determine this
        #NOTE: on smartos stderr, on linux stdout
        ( stdout, stderr )=self.zfs_node.run([ "zfs", "send", "-t", resume_token, "-n", "-v" ], valid_exitcodes=[ 0, 255 ], readonly=True, return_stderr=True )
        if stdout:
            lines=stdout
        else:
            lines=stderr
        for line in lines:
            matches=re.findall("toname = .*@(.*)", line)
            if matches:
                snapshot_name=matches[0]
                snapshot=ZfsDataset(self.zfs_node, self.filesystem_name+"@"+snapshot_name)
                snapshot.debug("resume token belongs to this snapshot")
                return(snapshot)

        return(None)


    def thin_list(self, keeps=[], ignores=[]):
        """determines list of snapshots that should be kept or deleted based on the thinning schedule. cull the herd!
        keep: list of snapshots to always keep (usually the last)
        ignores: snapshots to completely ignore (usually incompatible target snapshots that are going to be destroyed anyway)

        returns: ( keeps, obsoletes )
        """

        snapshots=[snapshot for snapshot in self.our_snapshots if snapshot not in ignores]

        return(self.zfs_node.thinner.thin(snapshots, keep_objects=keeps))


    def thin(self, skip_holds=False):
        """destroys snapshots according to thin_list, except last snapshot"""

        (keeps, obsoletes)=self.thin_list(keeps=self.our_snapshots[-1:])
        for obsolete in obsoletes:
            if skip_holds and obsolete.is_hold():
                obsolete.verbose("Keeping (common snapshot)")
            else:
                obsolete.destroy()
                self.snapshots.remove(obsolete)


    def find_common_snapshot(self, target_dataset):
        """find latest common snapshot between us and target
        returns None if its an initial transfer
        """
        if not target_dataset.snapshots:
            #target has nothing yet
            return(None)
        else:
            # snapshot=self.find_snapshot(target_dataset.snapshots[-1].snapshot_name)

            # if not snapshot:
            #try to common snapshot
            for source_snapshot in reversed(self.snapshots):
                if target_dataset.find_snapshot(source_snapshot):
                    source_snapshot.debug("common snapshot")
                    return(source_snapshot)
            target_dataset.error("Cant find common snapshot with source.")
            raise(Exception("You probably need to delete the target dataset to fix this."))


    def find_start_snapshot(self, common_snapshot, other_snapshots):
        """finds first snapshot to send"""

        if not common_snapshot:
            if not self.snapshots:
                start_snapshot=None
            else:
                #start from beginning
                start_snapshot=self.snapshots[0]

                if not start_snapshot.is_ours() and not other_snapshots:
                    # try to start at a snapshot thats ours
                    start_snapshot=self.find_next_snapshot(start_snapshot, other_snapshots)
        else:
            start_snapshot=self.find_next_snapshot(common_snapshot, other_snapshots)

        return(start_snapshot)


    def find_incompatible_snapshots(self, common_snapshot):
        """returns a list of snapshots that is incompatible for a zfs recv onto the common_snapshot.
        all direct followup snapshots with written=0 are compatible."""

        ret=[]

        if common_snapshot and self.snapshots:
            followup=True
            for snapshot in self.snapshots[self.find_snapshot_index(common_snapshot)+1:]:
                if not followup or int(snapshot.properties['written'])!=0:
                    followup=False
                    ret.append(snapshot)

        return(ret)


    def get_allowed_properties(self, filter_properties, set_properties):
        """only returns lists of allowed properties for this dataset type"""

        allowed_filter_properties=[]
        allowed_set_properties=[]
        illegal_properties=self.ILLEGAL_PROPERTIES[self.properties['type']]
        for set_property in set_properties:
            (property, value) = set_property.split("=")
            if property not in illegal_properties:
                allowed_set_properties.append(set_property)

        for filter_property in filter_properties:
            if filter_property not in illegal_properties:
                allowed_filter_properties.append(filter_property)

        return ( ( allowed_filter_properties, allowed_set_properties  )  )




    def sync_snapshots(self, target_dataset, features, show_progress=False,  filter_properties=[], set_properties=[], ignore_recv_exit_code=False, source_holds=True, rollback=False, raw=False, other_snapshots=False, no_send=False, destroy_incompatible=False):
        """sync this dataset's snapshots to target_dataset, while also thinning out old snapshots along the way."""

        #determine common and start snapshot 
        target_dataset.debug("Determining start snapshot")
        common_snapshot=self.find_common_snapshot(target_dataset)
        start_snapshot=self.find_start_snapshot(common_snapshot, other_snapshots)
        #should be destroyed before attempting zfs recv:
        incompatible_target_snapshots=target_dataset.find_incompatible_snapshots(common_snapshot) 

        #make target snapshot list the same as source, by adding virtual non-existing ones to the list.
        target_dataset.debug("Creating virtual target snapshots")
        source_snapshot=start_snapshot
        while source_snapshot:
            #create virtual target snapshot
            virtual_snapshot=ZfsDataset(target_dataset.zfs_node, target_dataset.filesystem_name+"@"+source_snapshot.snapshot_name,force_exists=False)
            target_dataset.snapshots.append(virtual_snapshot)
            source_snapshot=self.find_next_snapshot(source_snapshot, other_snapshots)


        #now let thinner decide what we want on both sides as final state (after all transfers are done)
        self.debug("Create thinning list")
        if self.our_snapshots:
            (source_keeps, source_obsoletes)=self.thin_list(keeps=[self.our_snapshots[-1]])
        else:
            source_keeps=[]
            source_obsoletes=[]

        if target_dataset.our_snapshots:
            (target_keeps, target_obsoletes)=target_dataset.thin_list(keeps=[target_dataset.our_snapshots[-1]], ignores=incompatible_target_snapshots)
        else:
            target_keeps=[]
            target_obsoletes=[]


        #on source: destroy all obsoletes before common. but after common, only delete snapshots that target also doesn't want to explicitly keep
        before_common=True
        for source_snapshot in self.snapshots:
            if common_snapshot and source_snapshot.snapshot_name==common_snapshot.snapshot_name:
                before_common=False
                #never destroy common snapshot
            else:
                target_snapshot=target_dataset.find_snapshot(source_snapshot)
                if (source_snapshot in source_obsoletes) and (before_common or (target_snapshot not in target_keeps)):
                    source_snapshot.destroy()


        #on target: destroy everything thats obsolete, except common_snapshot
        for target_snapshot in target_dataset.snapshots:
            if (target_snapshot in target_obsoletes) and (not common_snapshot or target_snapshot.snapshot_name!=common_snapshot.snapshot_name):
                if target_snapshot.exists:
                    target_snapshot.destroy()


        #now actually transfer the snapshots, if we want
        if no_send:
            return


       #resume?
        resume_token=None
        if 'receive_resume_token' in target_dataset.properties:
            resume_token=target_dataset.properties['receive_resume_token']
            #not valid anymore?
            resume_snapshot=self.get_resume_snapshot(resume_token)
            if not resume_snapshot or start_snapshot.snapshot_name!=resume_snapshot.snapshot_name:
                target_dataset.verbose("Cant resume, resume token no longer valid.")
                target_dataset.abort_resume()
                resume_token=None


        #incompatible target snapshots?
        if incompatible_target_snapshots:
            if not destroy_incompatible:
                for snapshot in incompatible_target_snapshots:
                    snapshot.error("Incompatible snapshot")
                raise(Exception("Please destroy incompatible snapshots or use --destroy-incompatible."))
            else:
                for snapshot in incompatible_target_snapshots:
                    snapshot.verbose("Incompatible snapshot")
                    snapshot.destroy()
                    target_dataset.snapshots.remove(snapshot)


        #rollback target to latest?
        if rollback:
            target_dataset.rollback()


        #now actually transfer the snapshots
        prev_source_snapshot=common_snapshot
        source_snapshot=start_snapshot
        while source_snapshot:
            target_snapshot=target_dataset.find_snapshot(source_snapshot) #still virtual

            #does target actually want it?
            if target_snapshot not in target_obsoletes:
                ( allowed_filter_properties, allowed_set_properties ) = self.get_allowed_properties(filter_properties, set_properties) #NOTE: should we let transfer_snapshot handle this?
                source_snapshot.transfer_snapshot(target_snapshot, features=features, prev_snapshot=prev_source_snapshot, show_progress=show_progress,   filter_properties=allowed_filter_properties, set_properties=allowed_set_properties, ignore_recv_exit_code=ignore_recv_exit_code, resume_token=resume_token, raw=raw)
                resume_token=None

                #hold the new common snapshots and release the previous ones
                target_snapshot.hold()
                if source_holds:
                    source_snapshot.hold()
                if prev_source_snapshot:
                    if source_holds:
                        prev_source_snapshot.release()
                    target_dataset.find_snapshot(prev_source_snapshot).release()

                # we may now destroy the previous source snapshot if its obsolete
                if prev_source_snapshot in source_obsoletes:
                    prev_source_snapshot.destroy()                    

                # destroy the previous target snapshot if obsolete (usually this is only the common_snapshot, the rest was already destroyed or will not be send)
                prev_target_snapshot=target_dataset.find_snapshot(prev_source_snapshot)
                if prev_target_snapshot in target_obsoletes:
                    prev_target_snapshot.destroy()

                prev_source_snapshot=source_snapshot
            else:
                source_snapshot.debug("skipped (target doesn't need it)")
                #was it actually a resume?
                if resume_token:
                    target_dataset.debug("aborting resume, since we don't want that snapshot anymore")
                    target_dataset.abort_resume()
                    resume_token=None


            source_snapshot=self.find_next_snapshot(source_snapshot, other_snapshots)




class ZfsNode(ExecuteNode):
    """a node that contains zfs datasets. implements global (systemwide/pool wide) zfs commands"""

    def __init__(self, backup_name, logger, ssh_config=None, ssh_to=None, readonly=False, description="", debug_output=False, thinner=Thinner()):
        self.backup_name=backup_name
        if not description and ssh_to:
            self.description=ssh_to
        else:
            self.description=description

        self.logger=logger

        if ssh_config:
            self.verbose("Using custom SSH config: {}".format(ssh_config))

        if ssh_to:
            self.verbose("Datasets on: {}".format(ssh_to))
        else:
            self.verbose("Datasets are local")

        rules=thinner.human_rules()
        if rules:
            for rule in rules:
                self.verbose(rule)
        else:
            self.verbose("Keep no old snaphots")

        self.thinner=thinner

        #list of ZfsPools
        self.__pools={}

        ExecuteNode.__init__(self, ssh_config=ssh_config, ssh_to=ssh_to, readonly=readonly, debug_output=debug_output)


    @cached_property
    def supported_send_options(self):
        """list of supported options, for optimizing sends"""
        #not every zfs implementation supports them all

        ret=[]
        for option in ["-L", "-e", "-c"  ]:
            if self.valid_command(["zfs","send", option, "zfs_autobackup_option_test"]):
                ret.append(option)
        return(ret)

    @cached_property
    def supported_recv_options(self):
        """list of supported options"""
        #not every zfs implementation supports them all

        ret=[]
        for option in ["-s" ]:
            if self.valid_command(["zfs","recv", option, "zfs_autobackup_option_test"]):
                ret.append(option)
        return(ret)


    def valid_command(self, cmd):
        """test if a specified zfs options are valid exit code. use this to determine support options"""

        try:
            self.run(cmd, hide_errors=True, valid_exitcodes=[0,1])
        except subprocess.CalledProcessError as e:
            return False

        return True


    #TODO: also create a get_zfs_dataset() function that stores all the objects in a dict. This should optimize caching a bit and is more consistent.
    def get_zfs_pool(self, name):
        """get a ZfsPool() object from specified name. stores objects internally to enable caching"""
        
        return(self.__pools.setdefault(name, ZfsPool(self, name)))


    def reset_progress(self):
        """reset progress output counters"""
        self._progress_total_bytes=0
        self._progress_start_time=time.time()


    def parse_zfs_progress(self, line, hide_errors, prefix):
        """try to parse progress output of zfs recv -Pv, and don't show it as error to the user """

        #is it progress output?
        progress_fields=line.rstrip().split("\t")

        if (line.find("nvlist version")==0 or
            line.find("resume token contents")==0 or
            len(progress_fields)!=1 or
            line.find("skipping ")==0 or
            re.match("send from .*estimated size is ", line)):

                #always output for debugging offcourse
                self.debug(prefix+line.rstrip())

                #actual useful info
                if len(progress_fields)>=3:
                    if progress_fields[0]=='full' or progress_fields[0]=='size':
                        self._progress_total_bytes=int(progress_fields[2])
                    elif progress_fields[0]=='incremental':
                        self._progress_total_bytes=int(progress_fields[3])
                    else:
                        bytes=int(progress_fields[1])
                        percentage=0
                        if self._progress_total_bytes:
                            percentage=min(100,int(bytes*100/self._progress_total_bytes))
                            speed=int(bytes/(time.time()-self._progress_start_time)/(1024*1024))
                            bytes_left=self._progress_total_bytes-bytes
                            minutes_left=int((bytes_left/(bytes/(time.time()-self._progress_start_time)))/60)

                            print(">>> {}% {}MB/s (total {}MB, {} minutes left)     \r".format(percentage, speed, int(self._progress_total_bytes/(1024*1024)), minutes_left), end='', file=sys.stderr)
                            sys.stderr.flush()

                return


        #still do the normal stderr output handling
        if hide_errors:
            self.debug(prefix+line.rstrip())
        else:
            self.error(prefix+line.rstrip())

    def _parse_stderr_pipe(self, line, hide_errors):
        self.parse_zfs_progress(line, hide_errors, "STDERR|> ")

    def _parse_stderr(self, line, hide_errors):
        self.parse_zfs_progress(line, hide_errors, "STDERR > ")

    def verbose(self,txt):
        self.logger.verbose("{} {}".format(self.description, txt))

    def error(self,txt,titles=[]):
        self.logger.error("{} {}".format(self.description, txt))

    def debug(self,txt, titles=[]):
        self.logger.debug("{} {}".format(self.description, txt))

    def new_snapshotname(self):
        """determine uniq new snapshotname"""
        return(self.backup_name+"-"+time.strftime("%Y%m%d%H%M%S"))


    def consistent_snapshot(self, datasets, snapshot_name, min_changed_bytes):
        """create a consistent (atomic) snapshot of specified datasets, per pool.

        """

        pools={}

        #collect snapshots that we want to make, per pool
        for dataset in datasets:
            if not dataset.is_changed_ours(min_changed_bytes):
                dataset.verbose("No changes since {}".format(dataset.our_snapshots[-1].snapshot_name))
                continue

            snapshot=ZfsDataset(dataset.zfs_node, dataset.name+"@"+snapshot_name)

            pool=dataset.split_path()[0]
            if not pool in pools:
                pools[pool]=[]

            pools[pool].append(snapshot)

            #add snapshot to cache (also useful in testmode)
            dataset.snapshots.append(snapshot) #NOTE: this will trigger zfs list

        if not pools:
            self.verbose("No changes anywhere: not creating snapshots.")
            return

        #create consistent snapshot per pool
        for (pool_name, snapshots) in pools.items():
            cmd=[ "zfs", "snapshot" ]


            cmd.extend(map(lambda snapshot: str(snapshot), snapshots))

            self.verbose("Creating snapshots {} in pool {}".format(snapshot_name, pool_name))
            self.run(cmd, readonly=False)


    @cached_property
    def selected_datasets(self):
        """determine filesystems that should be backupped by looking at the special autobackup-property, systemwide

           returns: list of ZfsDataset
        """

        self.debug("Getting selected datasets")

        #get all source filesystems that have the backup property
        lines=self.run(tab_split=True, readonly=True, cmd=[
            "zfs", "get", "-t",  "volume,filesystem", "-o", "name,value,source", "-s", "local,inherited", "-H", "autobackup:"+self.backup_name
        ])

        #determine filesystems that should be actually backupped
        selected_filesystems=[]
        direct_filesystems=[]
        for line in lines:
            (name,value,source)=line
            dataset=ZfsDataset(self, name)

            if value=="false":
                dataset.verbose("Ignored (disabled)")

            else:
                if source=="local" and ( value=="true" or value=="child"):
                    direct_filesystems.append(name)

                if source=="local" and value=="true":
                    dataset.verbose("Selected (direct selection)")
                    selected_filesystems.append(dataset)
                elif source.find("inherited from ")==0 and (value=="true" or value=="child"):
                    inherited_from=re.sub("^inherited from ", "", source)
                    if inherited_from in direct_filesystems:
                        selected_filesystems.append(dataset)
                        dataset.verbose("Selected (inherited selection)")
                    else:
                        dataset.debug("Ignored (already a backup)")
                else:
                    dataset.verbose("Ignored (only childs)")

        return(selected_filesystems)


class ZfsAutobackup:
    """main class"""
    def __init__(self,argv):

        parser = argparse.ArgumentParser(
            description=HEADER,
            epilog='When a filesystem fails, zfs_backup will continue and report the number of failures at that end. Also the exit code will indicate the number of failures.')
        parser.add_argument('--ssh-config', default=None, help='Custom ssh client config')
        parser.add_argument('--ssh-source', default=None, help='Source host to get backup from. (user@hostname) Default %(default)s.')
        parser.add_argument('--ssh-target', default=None, help='Target host to push backup to. (user@hostname) Default  %(default)s.')
        parser.add_argument('--keep-source', type=str, default="10,1d1w,1w1m,1m1y", help='Thinning schedule for old source snapshots. Default: %(default)s')
        parser.add_argument('--keep-target', type=str, default="10,1d1w,1w1m,1m1y", help='Thinning schedule for old target snapshots. Default: %(default)s')

        parser.add_argument('backup_name', metavar='backup-name',    help='Name of the backup (you should set the zfs property "autobackup:backup-name" to true on filesystems you want to backup')
        parser.add_argument('target_path', metavar='target-path', default=None, nargs='?',  help='Target ZFS filesystem (optional: if not specified, zfs-autobackup will only operate as snapshot-tool on source)')

        parser.add_argument('--other-snapshots', action='store_true', help='Send over other snapshots as well, not just the ones created by this tool.')
        parser.add_argument('--no-snapshot', action='store_true', help='Don\'t create new snapshots (useful for finishing uncompleted backups, or cleanups)')
        parser.add_argument('--no-send', action='store_true', help='Don\'t send snapshots (useful for cleanups, or if you want a serperate send-cronjob)')
        parser.add_argument('--min-change', type=int, default=1, help='Number of bytes written after which we consider a dataset changed (default %(default)s)')
        parser.add_argument('--allow-empty', action='store_true', help='If nothing has changed, still create empty snapshots. (same as --min-change=0)')
        parser.add_argument('--ignore-replicated', action='store_true',  help='Ignore datasets that seem to be replicated some other way. (No changes since lastest snapshot. Useful for proxmox HA replication)')
        parser.add_argument('--no-holds', action='store_true',  help='Don\'t lock snapshots on the source. (Useful to allow proxmox HA replication to switches nodes)')
        #not sure if this ever was useful:
        # parser.add_argument('--ignore-new', action='store_true',  help='Ignore filesystem if there are already newer snapshots for it on the target (use with caution)')

        parser.add_argument('--resume', action='store_true', help=argparse.SUPPRESS)
        parser.add_argument('--strip-path', default=0, type=int, help='Number of directories to strip from target path (use 1 when cloning zones between 2 SmartOS machines)')
        # parser.add_argument('--buffer', default="",  help='Use mbuffer with specified size to speedup zfs transfer. (e.g. --buffer 1G) Will also show nice progress output.')


        parser.add_argument('--clear-refreservation', action='store_true', help='Filter "refreservation" property. (recommended, safes space. same as --filter-properties refreservation)')
        parser.add_argument('--clear-mountpoint', action='store_true', help='Set property canmount=noauto for new datasets. (recommended, prevents mount conflicts. same as --set-properties canmount=noauto)')
        parser.add_argument('--filter-properties', type=str, help='List of properties to "filter" when receiving filesystems. (you can still restore them with zfs inherit -S)')
        parser.add_argument('--set-properties', type=str, help='List of propererties to override when receiving filesystems. (you can still restore them with zfs inherit -S)')
        parser.add_argument('--rollback', action='store_true', help='Rollback changes to the latest target snapshot before starting. (normally you can prevent changes by setting the readonly property on the target_path to on)')
        parser.add_argument('--destroy-incompatible', action='store_true', help='Destroy incompatible snapshots on target. Use with care! (implies --rollback)')
        parser.add_argument('--destroy-missing', type=str, default=None, help='Destroy datasets on target that are missing on the source. Specify the time since the last snapshot, e.g: --destroy-missing 30d')
        parser.add_argument('--ignore-transfer-errors', action='store_true', help='Ignore transfer errors (still checks if received filesystem exists. useful for acltype errors)')
        parser.add_argument('--raw', action='store_true', help='For encrypted datasets, send data exactly as it exists on disk.')


        parser.add_argument('--test', action='store_true', help='dont change anything, just show what would be done (still does all read-only operations)')
        parser.add_argument('--verbose', action='store_true', help='verbose output')
        parser.add_argument('--debug', action='store_true', help='Show zfs commands that are executed, stops after an exception.')
        parser.add_argument('--debug-output', action='store_true', help='Show zfs commands and their output/exit codes. (noisy)')
        parser.add_argument('--progress', action='store_true', help='show zfs progress output (to stderr). Enabled by default on ttys.')

        #note args is the only global variable we use, since its a global readonly setting anyway
        args = parser.parse_args(argv)

        self.args=args

        if sys.stderr.isatty():
            args.progress=True
       
        if args.debug_output:
            args.debug=True

        if self.args.test:
            self.args.verbose=True

        if args.allow_empty:
            args.min_change=0

        if args.destroy_incompatible:
            args.rollback=True

        self.log=Log(show_debug=self.args.debug, show_verbose=self.args.verbose)

        if args.resume:
            self.verbose("NOTE: The --resume option isn't needed anymore (its autodetected now)")


    def verbose(self,txt,titles=[]):
        self.log.verbose(txt)

    def error(self,txt,titles=[]):
        self.log.error(txt)

    def debug(self,txt, titles=[]):
        self.log.debug(txt)

    def set_title(self, title):
        self.log.verbose("")
        self.log.verbose("#### "+title)

    # sync datasets, or thin-only on both sides
    # target is needed for this.
    def sync_datasets(self, source_node, source_datasets):

        description="[Target]"

        self.set_title("Target settings")

        target_thinner=Thinner(self.args.keep_target)
        target_node=ZfsNode(self.args.backup_name, self, ssh_config=self.args.ssh_config, ssh_to=self.args.ssh_target, readonly=self.args.test, debug_output=self.args.debug_output, description=description, thinner=target_thinner)
        target_node.verbose("Receive datasets under: {}".format(self.args.target_path))

        if self.args.no_send:        
            self.set_title("Thinning source and target")
        else:
            self.set_title("Sending and thinning")

        #check if exists, to prevent vague errors
        target_dataset=ZfsDataset(target_node, self.args.target_path)
        if not target_dataset.exists:
            self.error("Target path '{}' does not exist. Please create this dataset first.".format(target_dataset))
            return(255)


        if self.args.filter_properties:
            filter_properties=self.args.filter_properties.split(",")
        else:
            filter_properties=[]

        if self.args.set_properties:
            set_properties=self.args.set_properties.split(",")
        else:
            set_properties=[]

        if self.args.clear_refreservation:
            filter_properties.append("refreservation")

        if self.args.clear_mountpoint:
            set_properties.append("canmount=noauto")

        #sync datasets
        fail_count=0
        target_datasets=[]
        for source_dataset in source_datasets:

            try:
                #determine corresponding target_dataset
                target_name=self.args.target_path + "/" + source_dataset.lstrip_path(self.args.strip_path)
                target_dataset=ZfsDataset(target_node, target_name)
                target_datasets.append(target_dataset)

                #ensure parents exists
                #TODO: this isnt perfect yet, in some cases it can create parents when it shouldn't.
                if not self.args.no_send and not target_dataset.parent in target_datasets and not target_dataset.parent.exists:
                    target_dataset.parent.create_filesystem(parents=True)

                #determine common zpool features
                source_features=source_node.get_zfs_pool(source_dataset.split_path()[0]).features
                target_features=target_node.get_zfs_pool(target_dataset.split_path()[0]).features
                common_features=source_features and target_features
                # source_dataset.debug("Common features: {}".format(common_features))

                source_dataset.sync_snapshots(target_dataset, show_progress=self.args.progress, features=common_features, filter_properties=filter_properties, set_properties=set_properties, ignore_recv_exit_code=self.args.ignore_transfer_errors, source_holds= not self.args.no_holds, rollback=self.args.rollback, raw=self.args.raw, other_snapshots=self.args.other_snapshots, no_send=self.args.no_send, destroy_incompatible=self.args.destroy_incompatible)
            except Exception as e:
                fail_count=fail_count+1
                source_dataset.error("FAILED: "+str(e))
                if self.args.debug:
                    raise

        self.thin_missing_targets(ZfsDataset(target_node, self.args.target_path), target_datasets)


        return(fail_count)


    def thin_missing_targets(self, target_dataset, used_target_datasets):
        """thin/destroy target datasets that are missing on the source."""

        self.debug("Thinning obsolete datasets")

        for dataset in target_dataset.recursive_datasets:
            try:
                if dataset not in used_target_datasets:
                    dataset.debug("Missing on source, thinning")
                    dataset.thin()

                    #destroy_missing enabled?
                    if self.args.destroy_missing!=None:

                        #cant do anything without our own snapshots
                        if not dataset.our_snapshots:
                            if dataset.datasets:
                                dataset.debug("Destroy missing: ignoring")
                            else:
                                dataset.verbose("Destroy missing: has no snapshots made by us. (please destroy manually)")
                        else:
                            #past the deadline?
                            deadline_ttl=ThinnerRule("0s"+self.args.destroy_missing).ttl
                            now=int(time.time())
                            if dataset.our_snapshots[-1].timestamp + deadline_ttl > now:
                                dataset.verbose("Destroy missing: Waiting for deadline.")
                            else:
                                
                                dataset.debug("Destroy missing: Removing our snapshots.")

                                #remove all our snaphots, except last, to safe space in case we fail later on
                                for snapshot in dataset.our_snapshots[:-1]:
                                    snapshot.destroy(fail_exception=True)

                                #does it have other snapshots?
                                has_others=False
                                for snapshot in dataset.snapshots:
                                    if not snapshot.is_ours():
                                        has_others=True
                                        break

                                if has_others: 
                                    dataset.verbose("Destroy missing: Still in use by other snapshots")
                                else:
                                    if dataset.datasets:
                                        dataset.verbose("Destroy missing: Still has children here.")
                                    else:
                                        dataset.verbose("Destroy missing.")
                                        dataset.our_snapshots[-1].destroy(fail_exception=True)
                                        dataset.destroy(fail_exception=True)
            
            except Exception as e:
                dataset.error("Error during destoy missing ({})".format(str(e)))




    def thin_source(self, source_datasets):

        self.set_title("Thinning source")

        for source_dataset in source_datasets:
            source_dataset.thin(skip_holds=True)


    def run(self):

        try:
            self.verbose (HEADER)

            if self.args.test:
                self.verbose("TEST MODE - SIMULATING WITHOUT MAKING ANY CHANGES")

            self.set_title("Source settings")

            description="[Source]"
            source_thinner=Thinner(self.args.keep_source)
            source_node=ZfsNode(self.args.backup_name, self, ssh_config=self.args.ssh_config, ssh_to=self.args.ssh_source, readonly=self.args.test, debug_output=self.args.debug_output, description=description, thinner=source_thinner)
            source_node.verbose("Selects all datasets that have property 'autobackup:{}=true' (or childs of datasets that have 'autobackup:{}=child')".format(self.args.backup_name, self.args.backup_name))

            self.set_title("Selecting")
            selected_source_datasets=source_node.selected_datasets
            if not selected_source_datasets:
                self.error("No source filesystems selected, please do a 'zfs set autobackup:{0}=true' on the source datasets you want to select.".format(self.args.backup_name))
                return(255)

            source_datasets=[]

            #filter out already replicated stuff?
            if not self.args.ignore_replicated:
                source_datasets=selected_source_datasets
            else:
                self.set_title("Filtering already replicated filesystems")
                for selected_source_dataset in selected_source_datasets:
                    if selected_source_dataset.is_changed(self.args.min_change):
                        source_datasets.append(selected_source_dataset)
                    else:
                        selected_source_dataset.verbose("Ignoring, already replicated")

            if not self.args.no_snapshot:
                self.set_title("Snapshotting")
                source_node.consistent_snapshot(source_datasets, source_node.new_snapshotname(), min_changed_bytes=self.args.min_change)

            #if target is specified, we sync the datasets, otherwise we just thin the source. (e.g. snapshot mode)
            if self.args.target_path:
                fail_count=self.sync_datasets(source_node, source_datasets)
            else:
                self.thin_source(source_datasets)
                fail_count=0


            if not fail_count:
                if self.args.test:
                    self.set_title("All tests successfull.")
                else:
                    self.set_title("All operations completed successfully")
                    if not self.args.target_path:
                        self.verbose("(No target_path specified, only operated as snapshot tool.)")

            else:
                if fail_count!=255:
                    self.error("{} failures!".format(fail_count))


            if self.args.test:
                self.verbose("")
                self.verbose("TEST MODE - DID NOT MAKE ANY CHANGES!")

            return(fail_count)

        except Exception as e:
            self.error("Exception: "+str(e))
            if self.args.debug:
                raise
            return(255)
        except KeyboardInterrupt as e:
            self.error("Aborted")
            return(255)


if __name__ == "__main__":
    zfs_autobackup=ZfsAutobackup(sys.argv[1:])
    sys.exit(zfs_autobackup.run())
