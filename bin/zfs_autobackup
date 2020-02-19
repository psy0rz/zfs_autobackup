#!/usr/bin/env python
# -*- coding: utf8 -*-

# (c)edwin@datux.nl  - Released under GPL
#
# Greetings from eth0 2019 :)

from __future__ import print_function

import os
import sys
import re
import traceback
import subprocess
import pprint
# import cStringIO
import time
import argparse
from pprint import pprint as p
import select


import imp
try:
    import colorama
    use_color=True
except ImportError:
    use_color=False

VERSION="3.0-rc3"


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

    def verbose(self, txt):
        if self.show_verbose:
            if use_color:
                print(colorama.Style.NORMAL+ "  "+txt+colorama.Style.RESET_ALL)
            else:
                print("  "+txt)

    def debug(self, txt):
        if self.show_debug:
            if use_color:
                print(colorama.Fore.GREEN+ "# "+txt+colorama.Style.RESET_ALL)
            else:
                print("# "+txt)





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

        self.human_str="Keep oldest of {} {}{}, delete after {} {}{}.".format(
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



# ######### Thinner testing code
# now=int(time.time())
#
# t=Thinner("1d1w,1w1m,1m6m,1y2y", always_keep=1)
#
# import random
#
# class Thing:
#     def __init__(self, timestamp):
#         self.timestamp=timestamp
#
#     def __str__(self):
#         age=now-self.timestamp
#         struct=time.localtime(self.timestamp)
#         return("{} ({} days old)".format(time.strftime("%Y-%m-%d %H:%M:%S",struct),int(age/(3600*24))))
#
# def test():
#     global now
#     things=[]
#
#     while True:
#         print("#################### {}".format(time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(now))))
#
#         (keeps, removes)=t.run(things, now)
#
#         print ("### KEEP ")
#         for thing in keeps:
#             print(thing)
#
#         print ("### REMOVE ")
#         for thing in removes:
#             print(thing)
#
#         things=keeps
#
#         #increase random amount of time and maybe add a thing
#         now=now+random.randint(0,160000)
#         if random.random()>=0:
#             things.append(Thing(now))
#
#         sys.stdin.readline()
#
# test()




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




class ExecuteNode:
    """an endpoint to execute local or remote commands via ssh"""


    def __init__(self, ssh_to=None, readonly=False, debug_output=False):
        """ssh_to: server you want to ssh to. none means local
           readonly: only execute commands that dont make any changes (usefull for testing-runs)
           debug_output: show output and exit codes of commands in debugging output.
        """

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

        readonly: make this True if the command doesnt make any changes and is safe to execute in testmode
        pipe: Instead of executing, return a pipe-handle to be used to input to another run() command. (just like a | in linux)
        input: Can be None, a string or a pipe-handle you got from another run()
        return_stderr: return both stdout and stderr as a tuple
        """

        encoded_cmd=[]

        #use ssh?
        if self.ssh_to != None:
            encoded_cmd.extend(["ssh".encode('utf-8'), self.ssh_to.encode('utf-8')])

            #make sure the command gets all the data in utf8 format:
            #(this is neccesary if LC_ALL=en_US.utf8 is not set in the environment)
            for arg in cmd:
                #add single quotes for remote commands to support spaces and other wierd stuff (remote commands are executed in a shell)
                encoded_cmd.append( ("'"+arg+"'").encode('utf-8'))

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
            stdin=None
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
            p.stdin.write(input)

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


        if self.debug_output:
            self.debug("EXIT   > {}".format(p.returncode))

        #handle piped process error output and exit codes
        if isinstance(input, subprocess.Popen):

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





class ZfsDataset():
    """a zfs dataset (filesystem/volume/snapshot/clone)
    Note that a dataset doesnt have to actually exist (yet/anymore)
    Also most properties are cached for performance-reasons, but also to allow --test to function correctly.

    """

    # illegal properties per dataset type. these will be removed from --set-properties and --filter-properties
    ILLEGAL_PROPERTIES={
        'filesystem': [ ],
        'volume': [ "canmount" ],
    }

    ZFS_MAX_UNCHANGED_BYTES=200000

    def __init__(self, zfs_node, name, force_exists=None):
        """name: full path of the zfs dataset
        exists: specifiy if you already know a dataset exists or not. for performance reasons. (othewise it will have to check with zfs list when needed)
        """
        self.zfs_node=zfs_node
        self.name=name #full name
        self.force_exists=force_exists

    def __repr__(self):
        return("{}: {}".format(self.zfs_node, self.name))

    def __str__(self):
        return(self.name)

    def __eq__(self, obj):
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


    def lstrip_path(self,count):
        """return name with first count components stripped"""
        return("/".join(self.name.split("/")[count:]))


    def rstrip_path(self,count):
        """return name with last count components stripped"""
        return("/".join(self.name.split("/")[:-count]))


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


    def find_our_prev_snapshot(self, snapshot):
        """find our previous snapshot in this dataset. None if it doesnt exist"""

        if self.is_snapshot:
            raise(Exception("Please call this on a dataset."))

        try:
            index=self.find_our_snapshot_index(snapshot)
            if index!=None and index>0:
                return(self.our_snapshots[index-1])
            else:
                return(None)
        except:
            return(None)


    def find_our_next_snapshot(self, snapshot):
        """find our next snapshot in this dataset. None if it doesnt exist"""

        if self.is_snapshot:
            raise(Exception("Please call this on a dataset."))

        try:
            index=self.find_our_snapshot_index(snapshot)
            if index!=None and index>=0 and index<len(self.our_snapshots)-1:
                return(self.our_snapshots[index+1])
            else:
                return(None)
        except:
            return(None)


    @cached_property
    def exists(self):
        """check if dataset exists.
        Use force to force a specific value to be cached, if you already know. Usefull for performance reasons"""


        if self.force_exists!=None:
            self.debug("Checking if filesystem exists: forced to {}".format(self.force_exists))
            return(self.force_exists)
        else:
            self.debug("Checking if filesystem exists")


        return(self.zfs_node.run(tab_split=True, cmd=[ "zfs", "list", self.name], readonly=True, valid_exitcodes=[ 0,1 ], hide_errors=True) and True)


    def create_filesystem(self, parents=False):
        """create a filesytem"""
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


    def is_changed(self):
        """dataset is changed since ANY latest snapshot ?"""
        self.debug("Checking if dataset is changed")

        #NOTE: filesystems can have a very small amount written without actual changes in some cases
        if int(self.properties['written'])<=self.ZFS_MAX_UNCHANGED_BYTES:
            return(False)
        else:
            return(True)


    def is_ours(self):
        """return true if this snapshot is created by this backup_nanme"""
        if re.match("^"+self.zfs_node.backup_name+"-[0-9]*$", self.snapshot_name):
            return(True)
        else:
            return(False)


    def hold(self):
        """hold dataset"""
        self.debug("holding")
        self.zfs_node.run([ "zfs" , "hold", "zfs_autobackup:"+self.zfs_node.backup_name, self.name ], valid_exitcodes=[ 0,1 ])

    def release(self):
        """release dataset"""
        self.debug("releasing")
        self.zfs_node.run([ "zfs" , "release", "zfs_autobackup:"+self.zfs_node.backup_name, self.name ], valid_exitcodes=[ 0,1 ])



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
        """find snapshot by snapshot (can be a snapshot_name or ZfsDataset)"""

        if not isinstance(snapshot,ZfsDataset):
            snapshot_name=snapshot
        else:
            snapshot_name=snapshot.snapshot_name

        for snapshot in self.our_snapshots:
            if snapshot.snapshot_name==snapshot_name:
                return(snapshot)

        return(None)


    def find_our_snapshot_index(self, snapshot):
        """find our snapshot index by snapshot (can be a snapshot_name or ZfsDataset)"""

        if not isinstance(snapshot,ZfsDataset):
            snapshot_name=snapshot
        else:
            snapshot_name=snapshot.snapshot_name

        index=0
        for snapshot in self.our_snapshots:
            if snapshot.snapshot_name==snapshot_name:
                return(index)
            index=index+1

        return(None)


    @cached_property
    def is_changed_ours(self):
        """dataset is changed since OUR latest snapshot?"""

        self.debug("Checking if dataset is changed since our snapshot")

        if not self.our_snapshots:
            return(True)

        latest_snapshot=self.our_snapshots[-1]

        cmd=[ "zfs", "get","-H" ,"-ovalue", "-p", "written@"+str(latest_snapshot), self.name ]
        output=self.zfs_node.run(readonly=True, tab_split=False, cmd=cmd, valid_exitcodes=[ 0 ])
        #NOTE: filesystems can have a very small amount written without actual changes in some cases
        if int(output[0])<=self.ZFS_MAX_UNCHANGED_BYTES:
            return(False)

        return(True)

    @cached_property
    def recursive_datasets(self, types="filesystem,volume"):
        """get all datasets recursively under us"""

        self.debug("Getting all datasets under us")

        names=self.zfs_node.run(tab_split=False, readonly=True, valid_exitcodes=[ 0 ], cmd=[
            "zfs", "list", "-r", "-t",  types, "-o", "name", "-H", self.name
        ])

        return(self.from_names(names[1:]))


    def send_pipe(self, prev_snapshot=None, resume=True, resume_token=None, show_progress=False, raw=False):
        """returns a pipe with zfs send output for this snapshot

        resume: Use resuming (both sides need to support it)
        resume_token: resume sending from this token. (in that case we dont need to know snapshot names)

        """
        #### build source command
        cmd=[]

        cmd.extend(["zfs", "send",  ])

        #all kind of performance options:
        cmd.append("-L") # large block support
        cmd.append("-e") # WRITE_EMBEDDED, more compact stream
        cmd.append("-c") # use compressed WRITE records
        if not resume:
            cmd.append("-D") # dedupped stream, sends less duplicate data

        #raw? (for encryption)
        if raw:
            cmd.append("--raw")


        #progress output
        if show_progress:
            cmd.append("-v")
            cmd.append("-P")


        #resume a previous send? (dont need more parameters in that case)
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

        #NOTE: this doenst start the send yet, it only returns a subprocess.Pipe
        return(self.zfs_node.run(cmd, pipe=True))


    def recv_pipe(self, pipe, resume=True, filter_properties=[], set_properties=[], ignore_exit_code=False):
        """starts a zfs recv for this snapshot and uses pipe as input

        note: you can also call both a snapshot and filesystem object.
        the resulting zfs command is the same, only our object cache is invalidated differently.
        """
        #### build target command
        cmd=[]

        cmd.extend(["zfs", "recv"])

        #dont mount filesystem that is received
        cmd.append("-u")

        for property in filter_properties:
            cmd.extend([ "-x" , property ])

        for property in set_properties:
            cmd.extend([ "-o" , property ])

        #verbose output
        cmd.append("-v")

        if resume:
            #support resuming
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
            raise(Exception("Target doesnt exist after transfer, something went wrong."))

        # if args.buffer and  args.ssh_target!="local":
        #     cmd.append("|mbuffer -m {}".format(args.buffer))


    def transfer_snapshot(self, target_snapshot, prev_snapshot=None, resume=True, show_progress=False, filter_properties=[], set_properties=[], ignore_recv_exit_code=False, resume_token=None, raw=False):
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
            #incemental
            target_snapshot.verbose("receiving incremental".format(self.snapshot_name))

        #do it
        pipe=self.send_pipe(resume=resume, show_progress=show_progress, prev_snapshot=prev_snapshot, resume_token=resume_token, raw=raw)
        target_snapshot.recv_pipe(pipe, resume=resume, filter_properties=filter_properties, set_properties=set_properties, ignore_exit_code=ignore_recv_exit_code)

    def abort_resume(self):
        """abort current resume state"""
        self.zfs_node.run(["zfs", "recv", "-A", self.name])


    def rollback(self):
        """rollback to this snapshot"""
        self.debug("Rolling back")
        self.zfs_node.run(["zfs", "rollback", self.name])


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




    def thin(self, keeps=[]):
        """determines list of snapshots that should be kept or deleted based on the thinning schedule. cull the herd!
        keep: list of snapshots to always keep (usually the last)

        returns: ( keeps, obsoletes )
        """
        return(self.zfs_node.thinner.thin(self.our_snapshots, keep_objects=keeps))


    def find_common_snapshot(self, target_dataset):
        """find latest coommon snapshot between us and target
        returns None if its an initial transfer
        """
        if not target_dataset.our_snapshots:
            #target has nothing yet
            return(None)
        else:
            snapshot=self.find_snapshot(target_dataset.our_snapshots[-1].snapshot_name)

            if not snapshot:
                #try to find another common snapshot as rollback-suggestion for admin
                for target_snapshot in reversed(target_dataset.our_snapshots):
                    if self.find_snapshot(target_snapshot):
                        target_snapshot.error("Latest common snapshot, roll back to this.")
                        raise(Exception("Cant find latest target snapshot on source."))
                target_dataset.error("Cant find common snapshot with target. ")
                raise(Exception("You probablly need to delete the target dataset to fix this."))


            snapshot.debug("common snapshot")

            return(snapshot)

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


    def sync_snapshots(self, target_dataset, show_progress=False, resume=True,  filter_properties=[], set_properties=[], ignore_recv_exit_code=False, source_holds=True, rollback=False, raw=False):
        """sync this dataset's snapshots to target_dataset,"""


        #determine start snapshot (the first snapshot after the common snapshot)
        target_dataset.debug("Determining start snapshot")
        common_snapshot=self.find_common_snapshot(target_dataset)
        if not common_snapshot:
            #start from beginning
            start_snapshot=self.our_snapshots[0]
        else:
            #roll target back to common snapshot
            if rollback:
                target_dataset.find_snapshot(common_snapshot).rollback()
            start_snapshot=self.find_our_next_snapshot(common_snapshot)

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


        #create virtual target snapshots
        target_dataset.debug("Creating virtual target snapshots")
        source_snapshot=start_snapshot
        while source_snapshot:
            #create virtual target snapshot
            virtual_snapshot=ZfsDataset(target_dataset.zfs_node, target_dataset.filesystem_name+"@"+source_snapshot.snapshot_name,force_exists=False)
            target_dataset.snapshots.append(virtual_snapshot)
            source_snapshot=self.find_our_next_snapshot(source_snapshot)

        #now let thinner decide what we want on both sides
        self.debug("Create thinning list")
        (source_keeps, source_obsoletes)=self.thin(keeps=[self.our_snapshots[-1]])
        (target_keeps, target_obsoletes)=target_dataset.thin(keeps=[target_dataset.our_snapshots[-1]])

        #stuff that is before common snapshot can be deleted rightaway
        if common_snapshot:
            for source_snapshot in self.our_snapshots:
                if source_snapshot.snapshot_name==common_snapshot.snapshot_name:
                    break

                if source_snapshot in source_obsoletes:
                    source_snapshot.destroy()

            for target_snapshot in target_dataset.our_snapshots:
                if target_snapshot.snapshot_name==common_snapshot.snapshot_name:
                    break

                if target_snapshot in target_obsoletes:
                    target_snapshot.destroy()

        #now send/destroy the rest off the source
        prev_source_snapshot=common_snapshot
        source_snapshot=start_snapshot
        while source_snapshot:
            target_snapshot=target_dataset.find_snapshot(source_snapshot) #"virtual"

            #does target actually want it?
            if target_snapshot in target_keeps:
                ( allowed_filter_properties, allowed_set_properties ) = self.get_allowed_properties(filter_properties, set_properties)
                source_snapshot.transfer_snapshot(target_snapshot, prev_snapshot=prev_source_snapshot, show_progress=show_progress, resume=resume,  filter_properties=allowed_filter_properties, set_properties=allowed_set_properties, ignore_recv_exit_code=ignore_recv_exit_code, resume_token=resume_token, raw=raw)
                resume_token=None

                #hold the new common snapshots and release the previous ones
                target_snapshot.hold()
                if source_holds:
                    source_snapshot.hold()
                if prev_source_snapshot:
                    if source_holds:
                        prev_source_snapshot.release()
                    target_dataset.find_snapshot(prev_source_snapshot).release()

                #we may destroy the previous snapshot now, if we dont want it anymore
                if prev_source_snapshot and (prev_source_snapshot not in source_keeps):
                    prev_source_snapshot.destroy()

                prev_source_snapshot=source_snapshot
            else:
                source_snapshot.debug("skipped (target doesnt need it)")
                #was it actually a resume?
                if resume_token:
                    target_dataset.debug("aborting resume, since we dont want that snapshot anymore")
                    target_dataset.abort_resume()
                    resume_token=None   

                #destroy it if we also dont want it anymore:
                if source_snapshot not in source_keeps:
                    source_snapshot.destroy()


            source_snapshot=self.find_our_next_snapshot(source_snapshot)




class ZfsNode(ExecuteNode):
    """a node that contains zfs datasets. implements global (systemwide/pool wide) zfs commands"""

    def __init__(self, backup_name, zfs_autobackup, ssh_to=None, readonly=False, description="", debug_output=False, thinner=Thinner()):
        self.backup_name=backup_name
        if not description:
            self.description=ssh_to
        else:
            self.description=description

        self.zfs_autobackup=zfs_autobackup #for logging

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


        ExecuteNode.__init__(self, ssh_to=ssh_to, readonly=readonly, debug_output=debug_output)


    def reset_progress(self):
        """reset progress output counters"""
        self._progress_total_bytes=0
        self._progress_start_time=time.time()

    def _parse_stderr_pipe(self, line, hide_errors):
        """try to parse progress output of a piped zfs recv -Pv """


        #is it progress output?
        progress_fields=line.rstrip().split("\t")

        if (line.find("nvlist version")==0 or
            line.find("resume token contents")==0 or
            len(progress_fields)!=1 or
            line.find("skipping ")==0):
 
                #always output for debugging offcourse
                self.debug("STDERR|> "+line.rstrip())

                #actual usefull info
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

                            print(">>> {}% {}MB/s (total {}MB, {} minutes left)  \r".format(percentage, speed, int(self._progress_total_bytes/(1024*1024)), minutes_left), end='')
                            sys.stdout.flush()

                return

            # #is it progress output?
            # if progress_output.find("nv")


        #normal output without progress stuff
        if hide_errors:
            self.debug("STDERR|> "+line.rstrip())
        else:
            self.error("STDERR|> "+line.rstrip())

    def verbose(self,txt):
        self.zfs_autobackup.verbose("{} {}".format(self.description, txt))

    def error(self,txt,titles=[]):
        self.zfs_autobackup.error("{} {}".format(self.description, txt))

    def debug(self,txt, titles=[]):
        self.zfs_autobackup.debug("{} {}".format(self.description, txt))

    def new_snapshotname(self):
        """determine uniq new snapshotname"""
        return(self.backup_name+"-"+time.strftime("%Y%m%d%H%M%S"))


    def consistent_snapshot(self, datasets, snapshot_name, allow_empty=True):
        """create a consistent (atomic) snapshot of specified datasets.

        allow_empty: Allow empty snapshots. (compared to our latest snapshot)
        """

        cmd=[ "zfs", "snapshot" ]

        noop=True
        for dataset in datasets:
            if not allow_empty:
                if not dataset.is_changed_ours:
                    dataset.verbose("No changes since {}".format(dataset.our_snapshots[-1].snapshot_name))
                    continue

            snapshot=ZfsDataset(dataset.zfs_node, dataset.name+"@"+snapshot_name)
            cmd.append(str(snapshot))

            #add snapshot to cache (also usefull in testmode)
            dataset.snapshots.append(snapshot)

            noop=False

        if noop:
            self.verbose("No changes, not creating snapshot.")
        else:
            self.verbose("Creating snapshot {}".format(snapshot_name))
            self.run(cmd, readonly=False)


    @cached_property
    def selected_datasets(self):
        """determine filesystems that should be backupped by looking at the special autobackup-property, systemwide

           returns: list of ZfsDataset
        """
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
                        dataset.verbose("Ignored (already a backup)")
                else:
                    dataset.verbose("Ignored (only childs)")

        return(selected_filesystems)







class ZfsAutobackup:
    """main class"""
    def __init__(self):

        parser = argparse.ArgumentParser(
            description='ZFS autobackup '+VERSION,
            epilog='When a filesystem fails, zfs_backup will continue and report the number of failures at that end. Also the exit code will indicate the number of failures.')
        parser.add_argument('--ssh-source', default=None, help='Source host to get backup from. (user@hostname) Default %(default)s.')
        parser.add_argument('--ssh-target', default=None, help='Target host to push backup to. (user@hostname) Default  %(default)s.')
        parser.add_argument('--keep-source', type=str, default="10,1d1w,1w1m,1m1y", help='Thinning schedule for old source snapshots. Default: %(default)s')
        parser.add_argument('--keep-target', type=str, default="10,1d1w,1w1m,1m1y", help='Thinning schedule for old target snapshots. Default: %(default)s')

        parser.add_argument('backup_name',    help='Name of the backup (you should set the zfs property "autobackup:backup-name" to true on filesystems you want to backup')
        parser.add_argument('target_path',    help='Target ZFS filesystem')

        parser.add_argument('--no-snapshot', action='store_true', help='dont create new snapshot (usefull for finishing uncompleted backups, or cleanups)')
        #Not appliciable anymore, version 3 alreadhy does optimal cleaning
        # parser.add_argument('--no-send', action='store_true', help='dont send snapshots (usefull to only do a cleanup)')
        parser.add_argument('--allow-empty', action='store_true', help='if nothing has changed, still create empty snapshots.')
        parser.add_argument('--ignore-replicated', action='store_true',  help='Ignore datasets that seem to be replicated some other way. (No changes since lastest snapshot. Usefull for proxmox HA replication)')
        parser.add_argument('--no-holds', action='store_true',  help='Dont lock snapshots on the source. (Usefull to allow proxmox HA replication to switches nodes)')
        #not sure if this ever was usefull:
        # parser.add_argument('--ignore-new', action='store_true',  help='Ignore filesystem if there are already newer snapshots for it on the target (use with caution)')

        parser.add_argument('--resume', action='store_true', help='support resuming of interrupted transfers by using the zfs extensible_dataset feature (both zpools should have it enabled) Disadvantage is that you need to use zfs recv -A if another snapshot is created on the target during a receive. Otherwise it will keep failing.')
        parser.add_argument('--strip-path', default=0, type=int, help='number of directory to strip from path (use 1 when cloning zones between 2 SmartOS machines)')
        parser.add_argument('--buffer', default="",  help='Use mbuffer with specified size to speedup zfs transfer. (e.g. --buffer 1G) Will also show nice progress output.')


        # parser.add_argument('--destroy-stale', action='store_true', help='Destroy stale backups that have no more snapshots. Be sure to verify the output before using this! ')
        parser.add_argument('--clear-refreservation', action='store_true', help='Filter "refreservation" property. (recommended, safes space. same as --filter-properties refreservation)')
        parser.add_argument('--clear-mountpoint', action='store_true', help='Filter "canmount" property. You still have to set canmount=noauto on the backup server. (recommended, prevents mount conflicts. same as --filter-properties canmount)')
        parser.add_argument('--filter-properties', type=str, help='List of propererties to "filter" when receiving filesystems. (you can still restore them with zfs inherit -S)')
        parser.add_argument('--set-properties', type=str, help='List of propererties to override when receiving filesystems. (you can still restore them with zfs inherit -S)')
        parser.add_argument('--rollback', action='store_true', help='Rollback changes on the target before starting a backup. (normally you can prevent changes by setting the readonly property on the target_path to on)')
        parser.add_argument('--ignore-transfer-errors', action='store_true', help='Ignore transfer errors (still checks if received filesystem exists. usefull for acltype errors)')
        parser.add_argument('--raw', action='store_true', help='For encrypted datasets, send data exactly as it exists on disk.')


        parser.add_argument('--test', action='store_true', help='dont change anything, just show what would be done (still does all read-only operations)')
        parser.add_argument('--verbose', action='store_true', help='verbose output')
        parser.add_argument('--debug', action='store_true', help='Show zfs commands that are executed, stops after an exception.')
        parser.add_argument('--debug-output', action='store_true', help='Show zfs commands and their output/exit codes. (noisy)')
        parser.add_argument('--progress', action='store_true', help='show zfs progress output (to stderr)')

        #note args is the only global variable we use, since its a global readonly setting anyway
        args = parser.parse_args()

        self.args=args

        if args.debug_output:
            args.debug=True

        if self.args.test:
            self.args.verbose=True

        self.log=Log(show_debug=self.args.debug, show_verbose=self.args.verbose)


    def verbose(self,txt,titles=[]):
        self.log.verbose(txt)

    def error(self,txt,titles=[]):
        self.log.error(txt)

    def debug(self,txt, titles=[]):
        self.log.debug(txt)

    def set_title(self, title):
        self.log.verbose("")
        self.log.verbose("#### "+title)

    def run(self):
        if self.args.test:
            self.verbose("TEST MODE - SIMULATING WITHOUT MAKING ANY CHANGES")

        self.set_title("Settings summary")

        description="[Source]"
        source_thinner=Thinner(self.args.keep_source)
        source_node=ZfsNode(self.args.backup_name, self, ssh_to=self.args.ssh_source, readonly=self.args.test, debug_output=self.args.debug_output, description=description, thinner=source_thinner)
        source_node.verbose("Send all datasets that have 'autobackup:{}=true' or 'autobackup:{}=child'".format(self.args.backup_name, self.args.backup_name))

        self.verbose("")

        description="[Target]"
        target_thinner=Thinner(self.args.keep_target)
        target_node=ZfsNode(self.args.backup_name, self, ssh_to=self.args.ssh_target, readonly=self.args.test, debug_output=self.args.debug_output, description=description, thinner=target_thinner)
        target_node.verbose("Receive datasets under: {}".format(self.args.target_path))

        self.set_title("Selecting")
        selected_source_datasets=source_node.selected_datasets
        if not selected_source_datasets:
            self.error("No source filesystems selected, please do a 'zfs set autobackup:{0}=true' on {1}".format(self.args.backup_name, self.args.ssh_source))
            return(255)

        source_datasets=[]

        #filter out already replicated stuff?
        if not self.args.ignore_replicated:
            source_datasets=selected_source_datasets
        else:
            self.set_title("Filtering already replicated filesystems")
            for selected_source_dataset in selected_source_datasets:
                if selected_source_dataset.is_changed():
                    source_datasets.append(selected_source_dataset)
                else:
                    selected_source_dataset.verbose("Ignoring, already replicated")


        if not self.args.no_snapshot:
            self.set_title("Snapshotting")
            source_node.consistent_snapshot(source_datasets, source_node.new_snapshotname(), allow_empty=self.args.allow_empty)


        self.set_title("Transferring")

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
            filter_properties.append("canmount")

        fail_count=0
        for source_dataset in source_datasets:

            try:
                #determine corresponding target_dataset
                target_name=self.args.target_path + "/" + source_dataset.lstrip_path(self.args.strip_path)
                target_dataset=ZfsDataset(target_node, target_name)

                #ensure parents exists
                if not target_dataset.parent.exists:
                    target_dataset.parent.create_filesystem(parents=True)

                source_dataset.sync_snapshots(target_dataset, show_progress=self.args.progress, resume=self.args.resume, filter_properties=filter_properties, set_properties=set_properties, ignore_recv_exit_code=self.args.ignore_transfer_errors, source_holds= not self.args.no_holds, rollback=self.args.rollback, raw=self.args.raw)
            except Exception as e:
                fail_count=fail_count+1
                source_dataset.error("DATASET FAILED: "+str(e))
                if self.args.debug:
                    raise



        if not fail_count:
            if self.args.test:
                self.set_title("All tests successfull.")
            else:
                self.set_title("All backups completed succesfully")
        else:
            self.error("{} datasets failed!".format(fail_count))

        if self.args.test:
            self.verbose("TEST MODE - DID NOT MAKE ANY BACKUPS!")

        return(fail_count)

if __name__ == "__main__":
    zfs_autobackup=ZfsAutobackup()
    sys.exit(zfs_autobackup.run())


