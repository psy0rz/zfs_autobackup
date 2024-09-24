
import re
from datetime import datetime
import sys
import time

from .ExecuteNode import ExecuteError


class ZfsDataset:
    """a zfs dataset (filesystem/volume/snapshot/clone) Note that a dataset
    doesn't have to actually exist (yet/anymore) Also most properties are cached
    for performance-reasons, but also to allow --test to function correctly.
    """

    # illegal properties per dataset type. these will be removed from --set-properties and --filter-properties
    ILLEGAL_PROPERTIES = {
        'filesystem': [],
        'volume': ["canmount"],
    }

    def __init__(self, zfs_node, name, force_exists=None):
        """
        Args:
            :type zfs_node: ZfsNode
            :type name: str
            :type force_exists: bool
        """
        self.zfs_node = zfs_node
        self.name = name  # full name

        #caching
        self.__snapshots=None #type: None|list[ZfsDataset]
        self.__written_since_ours=None #type: None|int
        self.__exists_check=None #type: None|bool
        self.__properties=None #type: None|dict[str,str]
        self.__recursive_datasets=None #type: None|list[ZfsDataset]
        self.__datasets=None #type: None|list[ZfsDataset]

        self.invalidate_cache()
        self.force_exists = force_exists


    def invalidate_cache(self):
        """clear caches"""
        # CachedProperty.clear(self)
        self.force_exists = None
        self.__snapshots=None
        self.__written_since_ours=None
        self.__exists_check=None
        self.__properties=None
        self.__recursive_datasets=None
        self.__datasets=None

    def __repr__(self):
        return "{}: {}".format(self.zfs_node, self.name)

    def __str__(self):

        return self.name

    def __eq__(self, obj):
        if not isinstance(obj, ZfsDataset):
            return False

        return self.name == obj.name

    def verbose(self, txt):
        """
        Args:
            :type txt: str
        """
        self.zfs_node.verbose("{}: {}".format(self.name, txt))

    def error(self, txt):
        """
        Args:
            :type txt: str
        """
        self.zfs_node.error("{}: {}".format(self.name, txt))

    def warning(self, txt):
        """
        Args:
            :type txt: str
        """
        self.zfs_node.warning("{}: {}".format(self.name, txt))

    def debug(self, txt):
        """
        Args:
            :type txt: str
        """
        self.zfs_node.debug("{}: {}".format(self.name, txt))


    def split_path(self):
        """return the path elements as an array"""
        return self.name.split("/")

    def lstrip_path(self, count):
        """return name with first count components stripped

        Args:
            :type count: int
        """
        components = self.split_path()
        if count > len(components):
            raise Exception("Trying to strip too much from path ({} items from {})".format(count, self.name))

        return "/".join(components[count:])

    def rstrip_path(self, count):
        """return name with last count components stripped

        Args:
            :type count: int
        """
        return "/".join(self.split_path()[:-count])

    @property
    def filesystem_name(self):
        """filesystem part of the name (before the @)"""
        if self.is_snapshot:
            (filesystem, snapshot) = self.name.split("@")
            return filesystem
        else:
            return self.name

    @property
    def snapshot_name(self):
        """snapshot part of the name"""
        if not self.is_snapshot:
            raise (Exception("This is not a snapshot"))

        (filesystem, snapshot_name) = self.name.split("@")
        return snapshot_name

    @property
    def is_snapshot(self):
        """true if this dataset is a snapshot"""
        return self.name.find("@") != -1

    @property
    def is_excluded(self):
        """true if this dataset is a snapshot and matches the exclude pattern"""
        if not self.is_snapshot:
            return False


        for pattern in self.zfs_node.exclude_snapshot_patterns:
            if pattern.search(self.name) is not None:
                self.debug("Excluded (path matches snapshot exclude pattern)")
                return True



    def is_selected(self, value, source, inherited, exclude_received, exclude_paths, exclude_unchanged):
        """determine if dataset should be selected for backup (called from
        ZfsNode)

        Args:
            :type exclude_paths: list[str]
            :type value: str
            :type source: str
            :type inherited: bool
            :type exclude_received: bool
            :type exclude_unchanged: int

            :param value: Value of the zfs property ("false"/"true"/"child"/parent/"-")
            :param source: Source of the zfs property ("local"/"received", "-")
            :param inherited: True of the value/source was inherited from a higher dataset.

        Returns: True : Selected
                 False: Excluded
                 None: No property found
        """

        # sanity checks
        if source not in ["local", "received", "-"]:
            # probably a program error in zfs-autobackup or new feature in zfs
            raise (Exception(
                "{} autobackup-property has illegal source: '{}' (possible BUG)".format(self.name, source)))

        if value not in ["false", "true", "child", "parent", "-"]:
            # user error
            raise (Exception(
                "{} autobackup-property has illegal value: '{}'".format(self.name, value)))

        # non specified, ignore
        if value == "-":
            return None

        # only select childs of this dataset, ignore
        if value == "child" and not inherited:
            return False

        # only select parent, no childs, ignore
        if value == "parent" and inherited:
            return False

        # manually excluded by property
        if value == "false":
            self.verbose("Excluded")
            return False

        # from here on the dataset is selected by property, now do additional exclusion checks

        # our path starts with one of the excluded paths?
        for exclude_path in exclude_paths:
            # if self.name.startswith(exclude_path):
            if (self.name + "/").startswith(exclude_path + "/"):
                # too noisy for verbose
                self.debug("Excluded (path in exclude list)")
                return False

        if source == "received":
            if exclude_received:
                self.verbose("Excluded (dataset already received)")
                return False

        if not self.is_changed(exclude_unchanged):
            self.verbose("Excluded (by --exclude-unchanged)")
            return False

        self.verbose("Selected")
        return True

    @property
    def parent(self):
        """get zfs-parent of this dataset. for snapshots this means it will get
        the filesystem/volume that it belongs to. otherwise it will return the
        parent according to path

        we cache this so everything in the parent that is cached also stays.

        returns None if there is no parent.
        :rtype: ZfsDataset | None
        """
        if self.is_snapshot:
            return self.zfs_node.get_dataset(self.filesystem_name)
        else:
            stripped = self.rstrip_path(1)
            if stripped:
                return self.zfs_node.get_dataset(stripped)
            else:
                return None

    # NOTE: unused for now
    # def find_prev_snapshot(self, snapshot, also_other_snapshots=False):
    #     """find previous snapshot in this dataset. None if it doesn't exist.
    #
    #     also_other_snapshots: set to true to also return snapshots that where
    #     not created by us. (is_ours)
    #
    #     Args:
    #         :type snapshot: str or ZfsDataset.ZfsDataset
    #         :type also_other_snapshots: bool
    #     """
    #
    #     if self.is_snapshot:
    #         raise (Exception("Please call this on a dataset."))
    #
    #     index = self.find_snapshot_index(snapshot)
    #     while index:
    #         index = index - 1
    #         if also_other_snapshots or self.snapshots[index].is_ours():
    #             return self.snapshots[index]
    #     return None

    def find_next_snapshot(self, snapshot):
        """find next snapshot in this dataset. None if it doesn't exist

        Args:
            :type snapshot: ZfsDataset
        """

        if self.is_snapshot:
            raise (Exception("Please call this on a dataset."))

        index = self.find_snapshot_index(snapshot)
        while index is not None and index < len(self.snapshots) - 1:
            index = index + 1
            return self.snapshots[index]
        return None

    @property
    def exists_check(self):
        """check on disk if it exists"""

        if self.__exists_check is None:
            self.debug("Checking if dataset exists")
            self.__exists_check=(len(self.zfs_node.run(tab_split=True, cmd=["zfs", "list", self.name], readonly=True,
                                          valid_exitcodes=[0, 1],
                                          hide_errors=True)) > 0)

        return self.__exists_check

    @property
    def exists(self):
        """returns True if dataset should exist.
           Use force_exists to force a specific value, if you already know. Useful for performance and test reasons
        """

        if self.force_exists is not None:
            if self.force_exists:
                self.debug("Dataset should exist")
            else:
                self.debug("Dataset should not exist")
            return self.force_exists
        else:
            return self.exists_check

    def create_filesystem(self, parents=False, unmountable=True):
        """create a filesystem

        Args:
            :type parents: bool
        """

        # recurse up
        if parents and self.parent and not self.parent.exists:
            self.parent.create_filesystem(parents, unmountable)

        cmd = ["zfs", "create"]

        if unmountable:
            cmd.extend(["-o", "canmount=off"])

        cmd.append(self.name)
        self.zfs_node.run(cmd)

        self.force_exists = True

    def destroy(self, fail_exception=False, deferred=False, verbose=True):
        """destroy the dataset. by default failures are not an exception, so we
        can continue making backups

        Args:
            :type fail_exception: bool
        """

        if verbose:
            self.verbose("Destroying")
        else:
            self.debug("Destroying")

        if self.is_snapshot:
            self.release()

        try:
            if deferred and self.is_snapshot:
                self.zfs_node.run(["zfs", "destroy", "-d", self.name])
            else:
                self.zfs_node.run(["zfs", "destroy", self.name])

            self.invalidate_cache()
            self.force_exists = False
            return True
        except ExecuteError:
            if not fail_exception:
                return False
            else:
                raise

    @property
    def properties(self):
        """all zfs properties"""

        if self.__properties is None:

            cmd = [
                "zfs", "get", "-H", "-o", "property,value", "-p", "all", self.name
            ]

            self.debug("Getting zfs properties")

            self.__properties = {}
            for pair in self.zfs_node.run(tab_split=True, cmd=cmd, readonly=True, valid_exitcodes=[0]):
                if len(pair) == 2:
                    self.__properties[pair[0]] = pair[1]

        return self.__properties

    def is_changed(self, min_changed_bytes=1):
        """dataset is changed since ANY latest snapshot ?

        Args:
            :type min_changed_bytes: int
        """
        self.debug("Checking if dataset is changed")

        if min_changed_bytes == 0:
            return True

        if int(self.properties['written']) < min_changed_bytes:
            return False
        else:
            return True

    def is_ours(self):
        """return true if this snapshot name belong to the current backup_name and snapshot formatting"""
        return self.timestamp is not None

    @property
    def holds(self):
        """get list[holds] for dataset"""

        output = self.zfs_node.run(["zfs", "holds", "-H", self.name], valid_exitcodes=[0], tab_split=True,
                                   readonly=True)
        return map(lambda fields: fields[1], output)

    def is_hold(self):
        """did we hold this snapshot?"""
        return self.zfs_node.hold_name in self.holds

    def hold(self):
        """hold dataset"""
        self.debug("holding")
        self.zfs_node.run(["zfs", "hold", self.zfs_node.hold_name, self.name], valid_exitcodes=[0, 1])

    def release(self):
        """release dataset"""
        if self.zfs_node.readonly or self.is_hold():
            self.debug("releasing")
            self.zfs_node.run(["zfs", "release", self.zfs_node.hold_name, self.name], valid_exitcodes=[0, 1])

    @property
    def timestamp(self):
        """get timestamp from snapshot name. Only works for our own snapshots
        with the correct format. Snapshots that are not ours always return None

        :rtype: int|None
        """

        try:
            dt = datetime.strptime(self.snapshot_name, self.zfs_node.snapshot_time_format)
        except ValueError:
            return None

        if sys.version_info[0] >= 3:
            from datetime import timezone
            if self.zfs_node.utc:
                dt = dt.replace(tzinfo=timezone.utc)
            seconds = dt.timestamp()
        else:
            # python2 has no good functions to deal with UTC. Yet the unix timestamp
            # must be in UTC to allow comparison against `time.time()` in on other parts
            # of this project (e.g. Thinner.py). If we are handling UTC timestamps,
            # we must adjust for that here.
            if self.zfs_node.utc:
                seconds = (dt - datetime(1970, 1, 1)).total_seconds()
            else:
                seconds = time.mktime(dt.timetuple())
        return seconds


    # def add_virtual_snapshot(self, snapshot):
    #     """pretend a snapshot exists (usefull in test mode)"""
    #
    #     # NOTE: we could just call self.snapshots.append() but this would trigger a zfs list which is not always needed.
    #     if CachedProperty.is_cached(self, 'snapshots'):
    #         # already cached so add it
    #         print ("ADDED")
    #         self.snapshots.append(snapshot)
    #     else:
    #         # self.snapshots will add it when requested
    #         print ("ADDED VIRT")
    #         self._virtual_snapshots.append(snapshot)

    @property
    def snapshots(self):
        """get all snapshots of this dataset
        :rtype: list[ZfsDataset]
        """

        #cached?
        if self.__snapshots is None:


            self.debug("Getting snapshots")

            cmd = [
                "zfs", "list", "-d", "1", "-r", "-t", "snapshot", "-H", "-o", "name", self.name
            ]

            self.__snapshots=self.zfs_node.get_datasets(self.zfs_node.run(cmd=cmd, readonly=True), force_exists=True)


        return self.__snapshots

    def cache_snapshot(self, snapshot):
        """Update our snapshot cache (if we have any)
        Args:
            :type snapshot: ZfsDataset
        """

        if self.__snapshots is not None:
            self.__snapshots.append(snapshot)

    @property
    def our_snapshots(self):
        """get list[snapshots] creates by us of this dataset"""
        ret = []

        for snapshot in self.snapshots:
            if snapshot.is_ours():
                ret.append(snapshot)

        return ret

    def find_snapshot_in_list(self, snapshots):
        """return ZfsDataset from the list of snapshots, if it matches the snapshot_name. Otherwise None
        Args:
            :type snapshots: list[ZfsDataset]
            :rtype: ZfsDataset|None
        """

        for snapshot in snapshots:
            if snapshot.snapshot_name==self.snapshot_name:
                return snapshot

        return None

    def find_snapshot(self, snapshot):
        """find snapshot by snapshot name (can be a snapshot_name or a different
        ZfsDataset) Returns None if it cant find it.

        Args:
            :rtype: ZfsDataset|None
            :type snapshot: str|ZfsDataset|None
        """

        if snapshot is None:
            return None

        if not isinstance(snapshot, ZfsDataset):
            snapshot_name = snapshot
        else:
            snapshot_name = snapshot.snapshot_name

        for snapshot in self.snapshots:
            if snapshot.snapshot_name == snapshot_name:
                return snapshot

        return None

    def find_snapshot_index(self, snapshot):
        """find snapshot index by snapshot (can be a snapshot_name or
        ZfsDataset)

        Args:
            :type snapshot: str or ZfsDataset
        """

        if not isinstance(snapshot, ZfsDataset):
            snapshot_name = snapshot
        else:
            snapshot_name = snapshot.snapshot_name

        index = 0
        for snapshot in self.snapshots:
            if snapshot.snapshot_name == snapshot_name:
                return index
            index = index + 1

        return None

    @property
    def written_since_ours(self):
        """get number of bytes written since our last snapshot"""

        if self.__written_since_ours is None:

            latest_snapshot = self.our_snapshots[-1]

            self.debug("Getting bytes written since our last snapshot")
            cmd = ["zfs", "get", "-H", "-ovalue", "-p", "written@" + str(latest_snapshot), self.name]

            output = self.zfs_node.run(readonly=True, tab_split=False, cmd=cmd, valid_exitcodes=[0])

            self.__written_since_ours=int(output[0])

        return self.__written_since_ours

    def is_changed_ours(self, min_changed_bytes=1):
        """dataset is changed since OUR latest snapshot?

        Args:
            :type min_changed_bytes: int
        """

        if min_changed_bytes == 0:
            return True

        if not self.our_snapshots:
            return True

        # NOTE: filesystems can have a very small amount written without actual changes in some cases
        if self.written_since_ours < min_changed_bytes:
            return False

        return True

    @property
    def recursive_datasets(self, types="filesystem,volume"):
        """get all (non-snapshot) datasets recursively under us

        Args:
            :type types: str
            :rtype: list[ZfsDataset]
        """

        if self.__recursive_datasets is None:

            self.debug("Getting all recursive datasets under us")

            names = self.zfs_node.run(tab_split=False, readonly=True, valid_exitcodes=[0], cmd=[
                "zfs", "list", "-r", "-t", types, "-o", "name", "-H", self.name
            ])

            self.__recursive_datasets=self.zfs_node.get_datasets(names[1:], force_exists=True)

        return self.__recursive_datasets

    @property
    def datasets(self, types="filesystem,volume"):
        """get all (non-snapshot) datasets directly under us

        Args:
            :type types: str
        """

        if self.__datasets is None:

            self.debug("Getting all datasets under us")

            names = self.zfs_node.run(tab_split=False, readonly=True, valid_exitcodes=[0], cmd=[
                "zfs", "list", "-r", "-t", types, "-o", "name", "-H", "-d", "1", self.name
            ])

            self.__datasets=self.zfs_node.get_datasets(names[1:], force_exists=True)

        return self.__datasets

    def send_pipe(self, features, prev_snapshot, resume_token, show_progress, raw, send_properties, write_embedded,
                  send_pipes, zfs_compressed):
        """returns a pipe with zfs send output for this snapshot

        resume_token: resume sending from this token. (in that case we don't
        need to know snapshot names)

        Args:
            :param send_pipes: output cmd array that will be added to actual zfs send command. (e.g. mbuffer or compression program)
            :type send_pipes: list[str]
            :type features: list[str]
            :type prev_snapshot: ZfsDataset
            :type resume_token: str
            :type show_progress: bool
            :type raw: bool
        """
        # build source command
        cmd = []

        cmd.extend(["zfs", "send", ])

        # all kind of performance options:
        if 'large_blocks' in features and "-L" in self.zfs_node.supported_send_options:
            # large block support (only if recordsize>128k which is seldomly used)
            cmd.append("-L")  # --large-block

        if write_embedded and 'embedded_data' in features and "-e" in self.zfs_node.supported_send_options:
            cmd.append("-e")  # --embed; WRITE_EMBEDDED, more compact stream

        if zfs_compressed and "-c" in self.zfs_node.supported_send_options:
            cmd.append("-c")  # --compressed; use compressed WRITE records

        # raw? (send over encrypted data in its original encrypted form without decrypting)
        if raw:
            cmd.append("--raw")

        # progress output
        if show_progress:
            cmd.append("-v")  # --verbose
            cmd.append("-P")  # --parsable

        # resume a previous send? (don't need more parameters in that case)
        if resume_token:
            cmd.extend(["-t", resume_token])

        else:
            # send properties
            if send_properties:
                cmd.append("-p")  # --props

            # incremental?
            if prev_snapshot:
                cmd.extend(["-i", "@" + prev_snapshot.snapshot_name])

            cmd.append(self.name)

        cmd.extend(send_pipes)

        output_pipe = self.zfs_node.run(cmd, pipe=True, readonly=True)

        return output_pipe

    def recv_pipe(self, pipe, features, recv_pipes, filter_properties=None, set_properties=None, ignore_exit_code=False,
                  force=False):
        """starts a zfs recv for this snapshot and uses pipe as input

        note: you can it both on a snapshot or filesystem object. The
        resulting zfs command is the same, only our object cache is invalidated
        differently.

        Args:
            :param recv_pipes: input cmd array that will be prepended to actual zfs recv command. (e.g. mbuffer or decompression program)
            :type pipe: subprocess.pOpen
            :type features: list[str]
            :type filter_properties: list[str]
            :type set_properties: list[str]
            :type ignore_exit_code: bool
        """

        if set_properties is None:
            set_properties = []

        if filter_properties is None:
            filter_properties = []

        # build target command
        cmd = []

        cmd.extend(recv_pipes)

        cmd.extend(["zfs", "recv"])

        # don't let zfs recv mount everything thats received (even with canmount=noauto!)
        cmd.append("-u")

        for property_ in filter_properties:
            cmd.extend(["-x", property_])

        for property_ in set_properties:
            cmd.extend(["-o", property_])

        # verbose output
        cmd.append("-v")

        if force:
            cmd.append("-F")

        if 'extensible_dataset' in features and "-s" in self.zfs_node.supported_recv_options:
            # support resuming
            self.debug("Enabled resume support")
            cmd.append("-s")

        cmd.append(self.filesystem_name)

        if ignore_exit_code:
            valid_exitcodes = []
        else:
            valid_exitcodes = [0]

        # self.zfs_node.reset_progress()
        self.zfs_node.run(cmd, inp=pipe, valid_exitcodes=valid_exitcodes)

        # invalidate cache
        self.invalidate_cache()

        # in test mode we assume everything was ok and it exists
        if self.zfs_node.readonly:
            self.force_exists = True

        # check if transfer was really ok (exit codes have been wrong before due to bugs in zfs-utils and some
        # errors should be ignored, thats where the ignore_exitcodes is for.)
        if not self.exists:
            self.error("error during transfer")
            raise (Exception("Target doesn't exist after transfer, something went wrong."))

        # at this point we're sure the actual dataset exists
        self.parent.force_exists = True

    def automount(self):
        """Mount the dataset as if one did a zfs mount -a, but only for this dataset
        Failure to mount doesnt result in an exception, but outputs errors to STDERR.

        """

        self.debug("Auto mounting")

        if self.properties['type'] != "filesystem":
            return

        if self.properties['canmount'] != 'on':
            return

        if self.properties['mountpoint'] == 'legacy':
            return

        if self.properties['mountpoint'] == 'none':
            return

        if self.properties['encryption'] != 'off' and self.properties['keystatus'] == 'unavailable':
            return

        self.zfs_node.run(["zfs", "mount", self.name], valid_exitcodes=[0,1])

    def transfer_snapshot(self, target_snapshot, features, prev_snapshot, show_progress,
                          filter_properties, set_properties, ignore_recv_exit_code, resume_token,
                          raw, send_properties, write_embedded, send_pipes, recv_pipes, zfs_compressed, force):
        """transfer this snapshot to target_snapshot. specify prev_snapshot for
        incremental transfer

        connects a send_pipe() to recv_pipe()

        Args:
            :type send_pipes: list[str]
            :type recv_pipes: list[str]
            :type target_snapshot: ZfsDataset
            :type features: list[str]
            :type prev_snapshot: ZfsDataset
            :type show_progress: bool
            :type filter_properties: list[str]
            :type set_properties: list[str]
            :type ignore_recv_exit_code: bool
            :type resume_token: str
            :type raw: bool
        """

        if set_properties is None:
            set_properties = []
        if filter_properties is None:
            filter_properties = []

        self.debug("Transfer snapshot to {}".format(target_snapshot.filesystem_name))

        if resume_token:
            self.verbose("resuming")

        # initial or increment
        if not prev_snapshot:
            self.verbose("-> {} (new)".format(target_snapshot.filesystem_name))
        else:
            # incremental
            self.verbose("-> {}".format(target_snapshot.filesystem_name))

        # do it
        pipe = self.send_pipe(features=features, show_progress=show_progress, prev_snapshot=prev_snapshot,
                              resume_token=resume_token, raw=raw, send_properties=send_properties,
                              write_embedded=write_embedded, send_pipes=send_pipes, zfs_compressed=zfs_compressed)
        target_snapshot.recv_pipe(pipe, features=features, filter_properties=filter_properties,
                                  set_properties=set_properties, ignore_exit_code=ignore_recv_exit_code,
                                  recv_pipes=recv_pipes, force=force)

        # try to automount it, if its the initial transfer
        if not prev_snapshot:
            # in test mode it doesnt actually exist, so dont try to mount it/read properties
            if not target_snapshot.zfs_node.readonly:
                target_snapshot.parent.automount()

    def abort_resume(self):
        """abort current resume state"""
        self.debug("Aborting resume")
        self.zfs_node.run(["zfs", "recv", "-A", self.name])

    def rollback(self):
        """rollback to latest existing snapshot on this dataset"""

        for snapshot in reversed(self.snapshots):
            if snapshot.exists:
                self.debug("Rolling back")
                self.zfs_node.run(["zfs", "rollback", snapshot.name])
                return

    def get_resume_snapshot(self, resume_token):
        """returns snapshot that will be resumed by this resume token (run this
        on source with target-token)

        Args:
            :type resume_token: str
        """
        # use zfs send -n option to determine this
        # NOTE: on smartos stderr, on linux stdout
        (stdout, stderr) = self.zfs_node.run(["zfs", "send", "-t", resume_token, "-n", "-v"], valid_exitcodes=[0, 255],
                                             readonly=True, return_stderr=True)
        if stdout:
            lines = stdout
        else:
            lines = stderr
        for line in lines:
            matches = re.findall("toname = .*@(.*)", line)
            if matches:
                snapshot_name = matches[0]
                snapshot = self.zfs_node.get_dataset(self.filesystem_name + "@" + snapshot_name)
                snapshot.debug("resume token belongs to this snapshot")
                return snapshot

        return None

    def thin_list(self, keeps=None, ignores=None):
        """determines list[snapshots] that should be kept or deleted based on
        the thinning schedule. cull the herd!

        returns: ( keeps, obsoletes )

        Args:
            :param keeps: list[snapshots] to always keep (usually the last)
            :param ignores: snapshots to completely ignore (usually incompatible target snapshots that are going to be destroyed anyway)
            :type keeps: list[ZfsDataset]
            :type ignores: list[ZfsDataset]
        """

        if ignores is None:
            ignores = []
        if keeps is None:
            keeps = []

        snapshots = [snapshot for snapshot in self.our_snapshots if snapshot not in ignores]

        return self.zfs_node.thin_list(snapshots, keep_snapshots=keeps)

    def thin(self, skip_holds=False):
        """destroys snapshots according to thin_list, except last snapshot

        Args:
            :type skip_holds: bool
        """

        (keeps, obsoletes) = self.thin_list(keeps=self.our_snapshots[-1:])
        for obsolete in obsoletes:
            if skip_holds and obsolete.is_hold():
                obsolete.verbose("Keeping (common snapshot)")
            else:
                obsolete.destroy()
                self.snapshots.remove(obsolete)

    def find_common_snapshot(self, target_dataset, guid_check):
        """find latest common snapshot between us and target returns None if its
        an initial transfer

        Args:
            :rtype: ZfsDataset|None
            :type guid_check: bool
            :type target_dataset: ZfsDataset
        """

        if not target_dataset.exists or not target_dataset.snapshots:
            # target has nothing yet
            return None
        else:
            for source_snapshot in reversed(self.snapshots):
                target_snapshot = target_dataset.find_snapshot(source_snapshot)
                if target_snapshot:
                    if guid_check and source_snapshot.properties['guid'] != target_snapshot.properties['guid']:
                        target_snapshot.warning("Common snapshots have mismatching GUID, ignoring.")
                    else:
                        target_snapshot.debug("common snapshot")
                        return source_snapshot
            # target_dataset.error("Cant find common snapshot with source.")
            raise (Exception("Cant find common snapshot with target."))


    def find_incompatible_snapshots(self, common_snapshot, raw):
        """returns a list[snapshots] that is incompatible for a zfs recv onto
        the common_snapshot. all direct followup snapshots with written=0 are
        compatible.

        in raw-mode nothing is compatible. issue #219

        Args:
            :type common_snapshot: ZfsDataset
            :type raw: bool
        """

        ret = []

        if common_snapshot and self.snapshots:
            followup = True
            for snapshot in self.snapshots[self.find_snapshot_index(common_snapshot) + 1:]:
                if raw or not followup or int(snapshot.properties['written']) != 0:
                    followup = False
                    ret.append(snapshot)

        return ret

    def get_allowed_properties(self, filter_properties, set_properties):
        """only returns lists of allowed properties for this dataset type

        Args:
            :type filter_properties: list[str]
            :type set_properties: list[str]
        """

        allowed_filter_properties = []
        allowed_set_properties = []
        illegal_properties = self.ILLEGAL_PROPERTIES[self.properties['type']]
        for set_property in set_properties:
            (property_, value) = set_property.split("=")
            if property_ not in illegal_properties:
                allowed_set_properties.append(set_property)

        for filter_property in filter_properties:
            if filter_property not in illegal_properties:
                allowed_filter_properties.append(filter_property)

        return allowed_filter_properties, allowed_set_properties


    def _pre_clean(self, source_common_snapshot, target_dataset, source_obsoletes, target_obsoletes, target_transfers):
        """cleanup old stuff before starting snapshot syncing

        Args:
            :type source_common_snapshot: ZfsDataset
            :type target_dataset: ZfsDataset
            :type source_obsoletes: list[ZfsDataset]
            :type target_obsoletes: list[ZfsDataset]
            :type target_transfers: list[ZfsDataset]
        """

        # on source: delete all obsoletes that are not in target_transfers (except common snapshot)
        for source_snapshot in self.snapshots:
            if (source_snapshot in source_obsoletes
                    and source_common_snapshot!=source_snapshot
                    and source_snapshot.find_snapshot_in_list(target_transfers) is None):
                source_snapshot.destroy()

        # on target: destroy everything thats obsolete, except common_snapshot
        if target_dataset.exists:
            for target_snapshot in target_dataset.snapshots:
                if (target_snapshot in target_obsoletes) \
                        and (not source_common_snapshot or (target_snapshot.snapshot_name != source_common_snapshot.snapshot_name)):
                    if target_snapshot.exists:
                        target_snapshot.destroy()

    def _validate_resume_token(self, target_dataset, start_snapshot):
        """validate and get (or destory) resume token

        Args:
            :type target_dataset: ZfsDataset
            :type start_snapshot: ZfsDataset
        """

        if target_dataset.exists and 'receive_resume_token' in target_dataset.properties:
            if start_snapshot is None:
                target_dataset.verbose("Aborting resume, its obsolete.")
                target_dataset.abort_resume()
            else:
                resume_token = target_dataset.properties['receive_resume_token']
                # not valid anymore
                resume_snapshot = self.get_resume_snapshot(resume_token)
                if not resume_snapshot or start_snapshot.snapshot_name != resume_snapshot.snapshot_name:
                    target_dataset.verbose("Aborting resume, its no longer valid.")
                    target_dataset.abort_resume()
                else:
                    return resume_token

    def _plan_sync(self, target_dataset, also_other_snapshots, guid_check, raw):
        """Determine at what snapshot to start syncing to target_dataset and what to sync and what to keep.

        Args:
            :rtype: ( ZfsDataset,  list[ZfsDataset], list[ZfsDataset], list[ZfsDataset], list[ZfsDataset] )
            :type target_dataset: ZfsDataset
            :type also_other_snapshots: bool
            :type guid_check: bool
            :type raw: bool

        Returns:
            tuple: A tuple containing:
                - ZfsDataset: The common snapshot
                - list[ZfsDataset]: Our obsolete source snapshots, after transfer is done. (will be thinned asap)
                - list[ZfsDataset]: Our obsolete target snapshots, after transfer is done. (will be thinned asap)
                - list[ZfsDataset]: Transfer target snapshots. These need to be transferred.
                - list[ZfsDataset]: Incompatible target snapshots. Target snapshots that are in the way, after the common snapshot. (need to be destroyed to continue)

        """

        ### 1: determine common and start snapshot
        target_dataset.debug("Determining start snapshot")
        source_common_snapshot = self.find_common_snapshot(target_dataset, guid_check=guid_check)
        incompatible_target_snapshots = target_dataset.find_incompatible_snapshots(source_common_snapshot, raw)

        # let thinner decide whats obsolete on source after the transfer is done, keeping the last snapshot as common.
        source_obsoletes = []
        if self.our_snapshots:
            source_obsoletes = self.thin_list(keeps=[self.our_snapshots[-1]])[1]

        ### 2: Determine possible target snapshots

        # start with snapshots that already exist, minus imcompatibles
        if target_dataset.exists:
            possible_target_snapshots = [snapshot for snapshot in target_dataset.snapshots if snapshot not in incompatible_target_snapshots]
        else:
            possible_target_snapshots = []

        # add all snapshots from the source, starting after the common snapshot if it exists
        if source_common_snapshot:
            source_snapshot=self.find_next_snapshot(source_common_snapshot )
        else:
            if self.snapshots:
                source_snapshot=self.snapshots[0]
            else:
                source_snapshot=None

        while source_snapshot:
            # we want it?
            if (also_other_snapshots or source_snapshot.is_ours()) and not source_snapshot.is_excluded:
                # create virtual target snapshot
                target_snapshot=target_dataset.zfs_node.get_dataset(target_dataset.filesystem_name + "@" + source_snapshot.snapshot_name, force_exists=False)
                possible_target_snapshots.append(target_snapshot)
            source_snapshot = self.find_next_snapshot(source_snapshot)

        ### 3: Let the thinner decide what it wants by looking at all the possible target_snaphots at once
        if possible_target_snapshots:
            (target_keeps, target_obsoletes)=target_dataset.zfs_node.thin_list(possible_target_snapshots, keep_snapshots=[possible_target_snapshots[-1]])
        else:
            target_keeps = []
            target_obsoletes = []

        ### 4: Look at what the thinner wants and create a list of snapshots we still need to transfer
        target_transfers=[]
        for target_keep in target_keeps:
            if not target_keep.exists:
                target_transfers.append(target_keep)

        return source_common_snapshot, source_obsoletes, target_obsoletes, target_transfers, incompatible_target_snapshots

    def handle_incompatible_snapshots(self, incompatible_target_snapshots, destroy_incompatible):
        """destroy incompatbile snapshots on target before sync, or inform user
        what to do

        Args:
            :type incompatible_target_snapshots: list[ZfsDataset]
            :type destroy_incompatible: bool
        """

        if incompatible_target_snapshots:
            if not destroy_incompatible:
                for snapshot in incompatible_target_snapshots:
                    snapshot.error("Incompatible snapshot")
                raise (Exception("Please destroy incompatible snapshots on target, or use --destroy-incompatible."))
            else:
                for snapshot in incompatible_target_snapshots:
                    snapshot.verbose("Incompatible snapshot")
                    snapshot.destroy(fail_exception=True)
                    self.snapshots.remove(snapshot)

                if len(incompatible_target_snapshots) > 0:
                    self.rollback()

    def sync_snapshots(self, target_dataset, features, show_progress, filter_properties, set_properties,
                       ignore_recv_exit_code, holds, rollback, decrypt, encrypt, also_other_snapshots,
                       no_send, destroy_incompatible, send_pipes, recv_pipes, zfs_compressed, force, guid_check):
        """sync this dataset's snapshots to target_dataset, while also thinning
        out old snapshots along the way.

        Args:
            :type send_pipes: list[str]
            :type recv_pipes: list[str]
            :type target_dataset: ZfsDataset
            :type features: list[str]
            :type show_progress: bool
            :type filter_properties: list[str]
            :type set_properties: list[str]
            :type ignore_recv_exit_code: bool
            :type holds: bool
            :type rollback: bool
            :type decrypt: bool
            :type also_other_snapshots: bool
            :type no_send: bool
            :type guid_check: bool
        """

        # self.verbose("-> {}".format(target_dataset))

        # defaults for these settings if there is no encryption stuff going on:
        send_properties = True
        raw = False
        write_embedded = True

        # source dataset encrypted?
        if self.properties.get('encryption', 'off') != 'off':
            # user wants to send it over decrypted?
            if decrypt:
                # when decrypting, zfs cant send properties
                send_properties = False
            else:
                # keep data encrypted by sending it raw (including properties)
                raw = True

        (source_common_snapshot,  source_obsoletes, target_obsoletes, target_transfers,
         incompatible_target_snapshots) = \
            self._plan_sync(target_dataset=target_dataset, also_other_snapshots=also_other_snapshots,
                            guid_check=guid_check, raw=raw)

        # NOTE: we do a pre-clean because we dont want filesystems to fillup when backups keep failing.
        # Also usefull with no_send to still cleanup stuff.
        self._pre_clean(
            source_common_snapshot=source_common_snapshot, target_dataset=target_dataset,
            target_transfers=target_transfers, target_obsoletes=target_obsoletes, source_obsoletes=source_obsoletes)

        # handle incompatible stuff on target
        target_dataset.handle_incompatible_snapshots(incompatible_target_snapshots, destroy_incompatible)

        # now actually transfer the snapshots, if we want
        if no_send or len(target_transfers)==0:
            return

        # check if we can resume
        resume_token = self._validate_resume_token(target_dataset, target_transfers[0])

        (active_filter_properties, active_set_properties) = self.get_allowed_properties(filter_properties,
                                                                                        set_properties)

        # encrypt at target?
        if encrypt and not raw:
            # filter out encryption properties to let encryption on the target take place
            active_filter_properties.extend(["keylocation", "pbkdf2iters", "keyformat", "encryption"])
            write_embedded = False

        # now actually transfer the snapshots

        do_rollback = rollback
        prev_source_snapshot=source_common_snapshot
        prev_target_snapshot=target_dataset.find_snapshot(source_common_snapshot)
        for target_snapshot in target_transfers:

            source_snapshot=self.find_snapshot(target_snapshot)

            # do the rollback, one time at first transfer
            if do_rollback:
                target_dataset.rollback()
                do_rollback = False

            source_snapshot.transfer_snapshot(target_snapshot, features=features,
                                              prev_snapshot=prev_source_snapshot, show_progress=show_progress,
                                              filter_properties=active_filter_properties,
                                              set_properties=active_set_properties,
                                              ignore_recv_exit_code=ignore_recv_exit_code,
                                              resume_token=resume_token, write_embedded=write_embedded, raw=raw,
                                              send_properties=send_properties, send_pipes=send_pipes,
                                              recv_pipes=recv_pipes, zfs_compressed=zfs_compressed, force=force)

            resume_token = None

            # hold the new common snapshots and release the previous ones
            if holds:
                target_snapshot.hold()
                source_snapshot.hold()

                if prev_source_snapshot:
                    prev_source_snapshot.release()

                if prev_target_snapshot:
                    prev_target_snapshot.release()

            # we may now destroy the previous source snapshot if its obsolete
            if prev_source_snapshot in source_obsoletes:
                prev_source_snapshot.destroy()

            # destroy the previous target snapshot if obsolete (usually this is only the common_snapshot,
            # the rest was already destroyed or will not be send)
            if prev_target_snapshot in target_obsoletes:
                prev_target_snapshot.destroy()

            prev_source_snapshot = source_snapshot
            prev_target_snapshot = target_snapshot

            # source_snapshot = self.find_next_snapshot(source_snapshot, also_other_snapshots)

    def mount(self, mount_point):

        self.debug("Mounting")

        cmd = [
            "mount", "-tzfs", self.name, mount_point
        ]

        self.zfs_node.run(cmd=cmd, valid_exitcodes=[0])

    def unmount(self, mount_point):

        self.debug("Unmounting")

        cmd = [
            "umount", mount_point
        ]

        self.zfs_node.run(cmd=cmd, valid_exitcodes=[0])

    def clone(self, name):
        """clones this snapshot and returns ZfsDataset of the clone"""

        self.debug("Cloning to {}".format(name))

        cmd = [
            "zfs", "clone", self.name, name
        ]

        self.zfs_node.run(cmd=cmd, valid_exitcodes=[0])

        return self.zfs_node.get_dataset(name, force_exists=True)

    def set(self, prop, value):
        """set a zfs property"""

        self.debug("Setting {}={}".format(prop, value))

        cmd = [
            "zfs", "set", "{}={}".format(prop, value), self.name
        ]

        self.zfs_node.run(cmd=cmd, valid_exitcodes=[0])

        #invalidate cache
        self.__properties=None

    def inherit(self, prop):
        """inherit zfs property"""

        self.debug("Inheriting property {}".format(prop))

        cmd = [
            "zfs", "inherit", prop, self.name
        ]

        self.zfs_node.run(cmd=cmd, valid_exitcodes=[0])

        #invalidate cache
        self.__properties=None
