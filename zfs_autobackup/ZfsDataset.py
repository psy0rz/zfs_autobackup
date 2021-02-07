import re
import subprocess
import time

from zfs_autobackup.CachedProperty import CachedProperty


class ZfsDataset:
    """a zfs dataset (filesystem/volume/snapshot/clone)
    Note that a dataset doesn't have to actually exist (yet/anymore)
    Also most properties are cached for performance-reasons, but also to allow --test to function correctly.

    """

    # illegal properties per dataset type. these will be removed from --set-properties and --filter-properties
    ILLEGAL_PROPERTIES = {
        'filesystem': [],
        'volume': ["canmount"],
    }

    def __init__(self, zfs_node, name, force_exists=None):
        """name: full path of the zfs dataset exists: specify if you already know a dataset exists or not. for
        performance and testing reasons. (otherwise it will have to check with zfs list when needed)
        """
        self.zfs_node = zfs_node
        self.name = name  # full name
        self._virtual_snapshots = []
        self.invalidate()
        self.force_exists = force_exists

    def __repr__(self):
        return "{}: {}".format(self.zfs_node, self.name)

    def __str__(self):
        return self.name

    def __eq__(self, obj):
        if not isinstance(obj, ZfsDataset):
            return False

        return self.name == obj.name

    def verbose(self, txt):
        self.zfs_node.verbose("{}: {}".format(self.name, txt))

    def error(self, txt):
        self.zfs_node.error("{}: {}".format(self.name, txt))

    def debug(self, txt):
        self.zfs_node.debug("{}: {}".format(self.name, txt))

    def invalidate(self):
        """clear caches"""
        CachedProperty.clear(self)
        self.force_exists = None
        self._virtual_snapshots = []

    def split_path(self):
        """return the path elements as an array"""
        return self.name.split("/")

    def lstrip_path(self, count):
        """return name with first count components stripped"""
        return "/".join(self.split_path()[count:])

    def rstrip_path(self, count):
        """return name with last count components stripped"""
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

    @CachedProperty
    def parent(self):
        """get zfs-parent of this dataset. for snapshots this means it will get the filesystem/volume that it belongs
        to. otherwise it will return the parent according to path

        we cache this so everything in the parent that is cached also stays.
        """
        if self.is_snapshot:
            return ZfsDataset(self.zfs_node, self.filesystem_name)
        else:
            return ZfsDataset(self.zfs_node, self.rstrip_path(1))

    def find_prev_snapshot(self, snapshot, other_snapshots=False):
        """find previous snapshot in this dataset. None if it doesn't exist.

        other_snapshots: set to true to also return snapshots that where not created by us. (is_ours)
        """

        if self.is_snapshot:
            raise (Exception("Please call this on a dataset."))

        index = self.find_snapshot_index(snapshot)
        while index:
            index = index - 1
            if other_snapshots or self.snapshots[index].is_ours():
                return self.snapshots[index]
        return None

    def find_next_snapshot(self, snapshot, other_snapshots=False):
        """find next snapshot in this dataset. None if it doesn't exist"""

        if self.is_snapshot:
            raise (Exception("Please call this on a dataset."))

        index = self.find_snapshot_index(snapshot)
        while index is not None and index < len(self.snapshots) - 1:
            index = index + 1
            if other_snapshots or self.snapshots[index].is_ours():
                return self.snapshots[index]
        return None

    @CachedProperty
    def exists(self):
        """check if dataset exists.
        Use force to force a specific value to be cached, if you already know. Useful for performance reasons"""

        if self.force_exists is not None:
            self.debug("Checking if filesystem exists: was forced to {}".format(self.force_exists))
            return self.force_exists
        else:
            self.debug("Checking if filesystem exists")

        return (self.zfs_node.run(tab_split=True, cmd=["zfs", "list", self.name], readonly=True, valid_exitcodes=[0, 1],
                                  hide_errors=True) and True)

    def create_filesystem(self, parents=False):
        """create a filesystem"""
        if parents:
            self.verbose("Creating filesystem and parents")
            self.zfs_node.run(["zfs", "create", "-p", self.name])
        else:
            self.verbose("Creating filesystem")
            self.zfs_node.run(["zfs", "create", self.name])

        self.force_exists = True

    def destroy(self, fail_exception=False):
        """destroy the dataset. by default failures are not an exception, so we can continue making backups"""

        self.verbose("Destroying")

        if self.is_snapshot:
            self.release()

        try:
            self.zfs_node.run(["zfs", "destroy", self.name])
            self.invalidate()
            self.force_exists = False
            return True
        except subprocess.CalledProcessError:
            if not fail_exception:
                return False
            else:
                raise

    @CachedProperty
    def properties(self):
        """all zfs properties"""

        cmd = [
            "zfs", "get", "-H", "-o", "property,value", "-p", "all", self.name
        ]

        if not self.exists:
            return {}

        self.debug("Getting zfs properties")

        ret = {}
        for pair in self.zfs_node.run(tab_split=True, cmd=cmd, readonly=True, valid_exitcodes=[0]):
            if len(pair) == 2:
                ret[pair[0]] = pair[1]

        return ret

    def is_changed(self, min_changed_bytes=1):
        """dataset is changed since ANY latest snapshot ?"""
        self.debug("Checking if dataset is changed")

        if min_changed_bytes == 0:
            return True

        if int(self.properties['written']) < min_changed_bytes:
            return False
        else:
            return True

    def is_ours(self):
        """return true if this snapshot is created by this backup_name"""
        if re.match("^" + self.zfs_node.backup_name + "-[0-9]*$", self.snapshot_name):
            return True
        else:
            return False

    @property
    def _hold_name(self):
        return "zfs_autobackup:" + self.zfs_node.backup_name

    @property
    def holds(self):
        """get list of holds for dataset"""

        output = self.zfs_node.run(["zfs", "holds", "-H", self.name], valid_exitcodes=[0], tab_split=True,
                                   readonly=True)
        return map(lambda fields: fields[1], output)

    def is_hold(self):
        """did we hold this snapshot?"""
        return self._hold_name in self.holds

    def hold(self):
        """hold dataset"""
        self.debug("holding")
        self.zfs_node.run(["zfs", "hold", self._hold_name, self.name], valid_exitcodes=[0, 1])

    def release(self):
        """release dataset"""
        if self.zfs_node.readonly or self.is_hold():
            self.debug("releasing")
            self.zfs_node.run(["zfs", "release", self._hold_name, self.name], valid_exitcodes=[0, 1])

    @property
    def timestamp(self):
        """get timestamp from snapshot name. Only works for our own snapshots with the correct format."""
        time_str = re.findall("^.*-([0-9]*)$", self.snapshot_name)[0]
        if len(time_str) != 14:
            raise (Exception("Snapshot has invalid timestamp in name: {}".format(self.snapshot_name)))

        # new format:
        time_secs = time.mktime(time.strptime(time_str, "%Y%m%d%H%M%S"))
        return time_secs

    def from_names(self, names):
        """convert a list of names to a list ZfsDatasets for this zfs_node"""
        ret = []
        for name in names:
            ret.append(ZfsDataset(self.zfs_node, name))

        return ret

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

    @CachedProperty
    def snapshots(self):
        """get all snapshots of this dataset"""


        if not self.exists:
            return []

        self.debug("Getting snapshots")

        cmd = [
            "zfs", "list", "-d", "1", "-r", "-t", "snapshot", "-H", "-o", "name", self.name
        ]

        return self.from_names(self.zfs_node.run(cmd=cmd, readonly=True))

    @property
    def our_snapshots(self):
        """get list of snapshots creates by us of this dataset"""
        ret = []
        for snapshot in self.snapshots:
            if snapshot.is_ours():
                ret.append(snapshot)

        return ret

    def find_snapshot(self, snapshot):
        """find snapshot by snapshot (can be a snapshot_name or a different ZfsDataset )"""

        if not isinstance(snapshot, ZfsDataset):
            snapshot_name = snapshot
        else:
            snapshot_name = snapshot.snapshot_name

        for snapshot in self.snapshots:
            if snapshot.snapshot_name == snapshot_name:
                return snapshot

        return None

    def find_snapshot_index(self, snapshot):
        """find snapshot index by snapshot (can be a snapshot_name or ZfsDataset)"""

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

    @CachedProperty
    def written_since_ours(self):
        """get number of bytes written since our last snapshot"""

        latest_snapshot = self.our_snapshots[-1]

        self.debug("Getting bytes written since our last snapshot")
        cmd = ["zfs", "get", "-H", "-ovalue", "-p", "written@" + str(latest_snapshot), self.name]

        output = self.zfs_node.run(readonly=True, tab_split=False, cmd=cmd, valid_exitcodes=[0])

        return int(output[0])

    def is_changed_ours(self, min_changed_bytes=1):
        """dataset is changed since OUR latest snapshot?"""

        if min_changed_bytes == 0:
            return True

        if not self.our_snapshots:
            return True

        # NOTE: filesystems can have a very small amount written without actual changes in some cases
        if self.written_since_ours < min_changed_bytes:
            return False

        return True

    @CachedProperty
    def recursive_datasets(self, types="filesystem,volume"):
        """get all (non-snapshot) datasets recursively under us"""

        self.debug("Getting all recursive datasets under us")

        names = self.zfs_node.run(tab_split=False, readonly=True, valid_exitcodes=[0], cmd=[
            "zfs", "list", "-r", "-t", types, "-o", "name", "-H", self.name
        ])

        return self.from_names(names[1:])

    @CachedProperty
    def datasets(self, types="filesystem,volume"):
        """get all (non-snapshot) datasets directly under us"""

        self.debug("Getting all datasets under us")

        names = self.zfs_node.run(tab_split=False, readonly=True, valid_exitcodes=[0], cmd=[
            "zfs", "list", "-r", "-t", types, "-o", "name", "-H", "-d", "1", self.name
        ])

        return self.from_names(names[1:])

    def send_pipe(self, features, prev_snapshot=None, resume_token=None, show_progress=False, raw=False):
        """returns a pipe with zfs send output for this snapshot

        resume_token: resume sending from this token. (in that case we don't need to know snapshot names)

        """
        # build source command
        cmd = []

        cmd.extend(["zfs", "send", ])

        # all kind of performance options:
        if 'large_blocks' in features and "-L" in self.zfs_node.supported_send_options:
            cmd.append("-L")  # large block support (only if recordsize>128k which is seldomly used)

        if 'embedded_data' in features and "-e" in self.zfs_node.supported_send_options:
            cmd.append("-e")  # WRITE_EMBEDDED, more compact stream

        if "-c" in self.zfs_node.supported_send_options:
            cmd.append("-c")  # use compressed WRITE records

        # NOTE: performance is usually worse with this option, according to manual
        # also -D will be depricated in newer ZFS versions
        # if not resume:
        #     if "-D" in self.zfs_node.supported_send_options:
        #         cmd.append("-D") # dedupped stream, sends less duplicate data

        # raw? (for encryption)
        if raw:
            cmd.append("--raw")

        # progress output
        if show_progress:
            cmd.append("-v")
            cmd.append("-P")

        # resume a previous send? (don't need more parameters in that case)
        if resume_token:
            cmd.extend(["-t", resume_token])

        else:
            # send properties
            cmd.append("-p")

            # incremental?
            if prev_snapshot:
                cmd.extend(["-i", "@" + prev_snapshot.snapshot_name])

            cmd.append(self.name)

        # if args.buffer and args.ssh_source!="local":
        #     cmd.append("|mbuffer -m {}".format(args.buffer))

        # NOTE: this doesn't start the send yet, it only returns a subprocess.Pipe
        return self.zfs_node.run(cmd, pipe=True)

    def recv_pipe(self, pipe, features, filter_properties=None, set_properties=None, ignore_exit_code=False):
        """starts a zfs recv for this snapshot and uses pipe as input

        note: you can it both on a snapshot or filesystem object.
        The resulting zfs command is the same, only our object cache is invalidated differently.
        """

        if set_properties is None:
            set_properties = []

        if filter_properties is None:
            filter_properties = []

        # build target command
        cmd = []

        cmd.extend(["zfs", "recv"])

        # don't mount filesystem that is received
        cmd.append("-u")

        for property_ in filter_properties:
            cmd.extend(["-x", property_])

        for property_ in set_properties:
            cmd.extend(["-o", property_])

        # verbose output
        cmd.append("-v")

        if 'extensible_dataset' in features and "-s" in self.zfs_node.supported_recv_options:
            # support resuming
            self.debug("Enabled resume support")
            cmd.append("-s")

        cmd.append(self.filesystem_name)

        if ignore_exit_code:
            valid_exitcodes = []
        else:
            valid_exitcodes = [0]

        self.zfs_node.reset_progress()
        self.zfs_node.run(cmd, inp=pipe, valid_exitcodes=valid_exitcodes)

        # invalidate cache, but we at least know we exist now
        self.invalidate()

        # in test mode we assume everything was ok and it exists
        if self.zfs_node.readonly:
            self.force_exists = True

        # check if transfer was really ok (exit codes have been wrong before due to bugs in zfs-utils and some
        # errors should be ignored, thats where the ignore_exitcodes is for.)
        if not self.exists:
            self.error("error during transfer")
            raise (Exception("Target doesn't exist after transfer, something went wrong."))

        # if args.buffer and  args.ssh_target!="local":
        #     cmd.append("|mbuffer -m {}".format(args.buffer))

    def transfer_snapshot(self, target_snapshot, features, prev_snapshot=None, show_progress=False,
                          filter_properties=None, set_properties=None, ignore_recv_exit_code=False, resume_token=None,
                          raw=False):
        """transfer this snapshot to target_snapshot. specify prev_snapshot for incremental transfer

        connects a send_pipe() to recv_pipe()
        """

        if set_properties is None:
            set_properties = []
        if filter_properties is None:
            filter_properties = []

        self.debug("Transfer snapshot to {}".format(target_snapshot.filesystem_name))

        if resume_token:
            target_snapshot.verbose("resuming")

        # initial or increment
        if not prev_snapshot:
            target_snapshot.verbose("receiving full".format(self.snapshot_name))
        else:
            # incremental
            target_snapshot.verbose("receiving incremental".format(self.snapshot_name))

        # do it
        pipe = self.send_pipe(features=features, show_progress=show_progress, prev_snapshot=prev_snapshot,
                              resume_token=resume_token, raw=raw)
        target_snapshot.recv_pipe(pipe, features=features, filter_properties=filter_properties,
                                  set_properties=set_properties, ignore_exit_code=ignore_recv_exit_code)

    def abort_resume(self):
        """abort current resume state"""
        self.zfs_node.run(["zfs", "recv", "-A", self.name])

    def rollback(self):
        """rollback to latest existing snapshot on this dataset"""

        for snapshot in reversed(self.snapshots):
            if snapshot.exists:
                self.debug("Rolling back")
                self.zfs_node.run(["zfs", "rollback", snapshot.name])
                return

    def get_resume_snapshot(self, resume_token):
        """returns snapshot that will be resumed by this resume token (run this on source with target-token)"""
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
                snapshot = ZfsDataset(self.zfs_node, self.filesystem_name + "@" + snapshot_name)
                snapshot.debug("resume token belongs to this snapshot")
                return snapshot

        return None

    def thin_list(self, keeps=None, ignores=None):
        """determines list of snapshots that should be kept or deleted based on the thinning schedule. cull the herd!
        keep: list of snapshots to always keep (usually the last) ignores: snapshots to completely ignore (usually
        incompatible target snapshots that are going to be destroyed anyway)

        returns: ( keeps, obsoletes )
        """

        if ignores is None:
            ignores = []
        if keeps is None:
            keeps = []

        snapshots = [snapshot for snapshot in self.our_snapshots if snapshot not in ignores]

        return self.zfs_node.thinner.thin(snapshots, keep_objects=keeps)

    def thin(self, skip_holds=False):
        """destroys snapshots according to thin_list, except last snapshot"""

        (keeps, obsoletes) = self.thin_list(keeps=self.our_snapshots[-1:])
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
            # target has nothing yet
            return None
        else:
            # snapshot=self.find_snapshot(target_dataset.snapshots[-1].snapshot_name)

            # if not snapshot:
            # try to common snapshot
            for source_snapshot in reversed(self.snapshots):
                if target_dataset.find_snapshot(source_snapshot):
                    source_snapshot.debug("common snapshot")
                    return source_snapshot
            target_dataset.error("Cant find common snapshot with source.")
            raise (Exception("You probably need to delete the target dataset to fix this."))

    def find_start_snapshot(self, common_snapshot, other_snapshots):
        """finds first snapshot to send"""

        if not common_snapshot:
            if not self.snapshots:
                start_snapshot = None
            else:
                # start from beginning
                start_snapshot = self.snapshots[0]

                if not start_snapshot.is_ours() and not other_snapshots:
                    # try to start at a snapshot thats ours
                    start_snapshot = self.find_next_snapshot(start_snapshot, other_snapshots)
        else:
            start_snapshot = self.find_next_snapshot(common_snapshot, other_snapshots)

        return start_snapshot

    def find_incompatible_snapshots(self, common_snapshot):
        """returns a list of snapshots that is incompatible for a zfs recv onto the common_snapshot.
        all direct followup snapshots with written=0 are compatible."""

        ret = []

        if common_snapshot and self.snapshots:
            followup = True
            for snapshot in self.snapshots[self.find_snapshot_index(common_snapshot) + 1:]:
                if not followup or int(snapshot.properties['written']) != 0:
                    followup = False
                    ret.append(snapshot)

        return ret

    def get_allowed_properties(self, filter_properties, set_properties):
        """only returns lists of allowed properties for this dataset type"""

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

    def sync_snapshots(self, target_dataset, features, show_progress=False, filter_properties=None, set_properties=None,
                       ignore_recv_exit_code=False, holds=True, rollback=False, raw=False, other_snapshots=False,
                       no_send=False, destroy_incompatible=False):
        """sync this dataset's snapshots to target_dataset, while also thinning out old snapshots along the way."""

        if set_properties is None:
            set_properties = []
        if filter_properties is None:
            filter_properties = []

        # determine common and start snapshot
        target_dataset.debug("Determining start snapshot")
        common_snapshot = self.find_common_snapshot(target_dataset)
        start_snapshot = self.find_start_snapshot(common_snapshot, other_snapshots)
        # should be destroyed before attempting zfs recv:
        incompatible_target_snapshots = target_dataset.find_incompatible_snapshots(common_snapshot)

        # make target snapshot list the same as source, by adding virtual non-existing ones to the list.
        target_dataset.debug("Creating virtual target snapshots")
        source_snapshot = start_snapshot
        while source_snapshot:
            # create virtual target snapshot
            virtual_snapshot = ZfsDataset(target_dataset.zfs_node,
                                          target_dataset.filesystem_name + "@" + source_snapshot.snapshot_name,
                                          force_exists=False)
            target_dataset.snapshots.append(virtual_snapshot)
            source_snapshot = self.find_next_snapshot(source_snapshot, other_snapshots)

        # now let thinner decide what we want on both sides as final state (after all transfers are done)
        if self.our_snapshots:
            self.debug("Create thinning list")
            (source_keeps, source_obsoletes) = self.thin_list(keeps=[self.our_snapshots[-1]])
        else:
            source_obsoletes = []

        if target_dataset.our_snapshots:
            (target_keeps, target_obsoletes) = target_dataset.thin_list(keeps=[target_dataset.our_snapshots[-1]],
                                                                        ignores=incompatible_target_snapshots)
        else:
            target_keeps = []
            target_obsoletes = []

        # on source: destroy all obsoletes before common. but after common, only delete snapshots that target also
        # doesn't want to explicitly keep
        before_common = True
        for source_snapshot in self.snapshots:
            if common_snapshot and source_snapshot.snapshot_name == common_snapshot.snapshot_name:
                before_common = False
                # never destroy common snapshot
            else:
                target_snapshot = target_dataset.find_snapshot(source_snapshot)
                if (source_snapshot in source_obsoletes) and (before_common or (target_snapshot not in target_keeps)):
                    source_snapshot.destroy()

        # on target: destroy everything thats obsolete, except common_snapshot
        for target_snapshot in target_dataset.snapshots:
            if (target_snapshot in target_obsoletes) and (
                    not common_snapshot or target_snapshot.snapshot_name != common_snapshot.snapshot_name):
                if target_snapshot.exists:
                    target_snapshot.destroy()

        # now actually transfer the snapshots, if we want
        if no_send:
            return

        # resume?
        resume_token = None
        if 'receive_resume_token' in target_dataset.properties:
            resume_token = target_dataset.properties['receive_resume_token']
            # not valid anymore?
            resume_snapshot = self.get_resume_snapshot(resume_token)
            if not resume_snapshot or start_snapshot.snapshot_name != resume_snapshot.snapshot_name:
                target_dataset.verbose("Cant resume, resume token no longer valid.")
                target_dataset.abort_resume()
                resume_token = None

        # incompatible target snapshots?
        if incompatible_target_snapshots:
            if not destroy_incompatible:
                for snapshot in incompatible_target_snapshots:
                    snapshot.error("Incompatible snapshot")
                raise (Exception("Please destroy incompatible snapshots or use --destroy-incompatible."))
            else:
                for snapshot in incompatible_target_snapshots:
                    snapshot.verbose("Incompatible snapshot")
                    snapshot.destroy()
                    target_dataset.snapshots.remove(snapshot)

        # rollback target to latest?
        if rollback:
            target_dataset.rollback()

        # now actually transfer the snapshots
        prev_source_snapshot = common_snapshot
        source_snapshot = start_snapshot
        while source_snapshot:
            target_snapshot = target_dataset.find_snapshot(source_snapshot)  # still virtual

            # does target actually want it?
            if target_snapshot not in target_obsoletes:
                # NOTE: should we let transfer_snapshot handle this?
                (allowed_filter_properties, allowed_set_properties) = self.get_allowed_properties(filter_properties,
                                                                                                  set_properties)
                source_snapshot.transfer_snapshot(target_snapshot, features=features,
                                                  prev_snapshot=prev_source_snapshot, show_progress=show_progress,
                                                  filter_properties=allowed_filter_properties,
                                                  set_properties=allowed_set_properties,
                                                  ignore_recv_exit_code=ignore_recv_exit_code,
                                                  resume_token=resume_token, raw=raw)
                resume_token = None

                # hold the new common snapshots and release the previous ones
                if holds:
                    target_snapshot.hold()
                    source_snapshot.hold()

                if prev_source_snapshot:
                    if holds:
                        prev_source_snapshot.release()
                        target_dataset.find_snapshot(prev_source_snapshot).release()

                # we may now destroy the previous source snapshot if its obsolete
                if prev_source_snapshot in source_obsoletes:
                    prev_source_snapshot.destroy()

                    # destroy the previous target snapshot if obsolete (usually this is only the common_snapshot,
                    # the rest was already destroyed or will not be send)
                prev_target_snapshot = target_dataset.find_snapshot(prev_source_snapshot)
                if prev_target_snapshot in target_obsoletes:
                    prev_target_snapshot.destroy()

                prev_source_snapshot = source_snapshot
            else:
                source_snapshot.debug("skipped (target doesn't need it)")
                # was it actually a resume?
                if resume_token:
                    target_dataset.debug("aborting resume, since we don't want that snapshot anymore")
                    target_dataset.abort_resume()
                    resume_token = None

            source_snapshot = self.find_next_snapshot(source_snapshot, other_snapshots)