from .ZfsDataset import ZfsDataset
from .ExecuteNode import ExecuteError
import re
from datetime import datetime
import sys
import time


class ZfsSnapshot(ZfsDataset):
    """A ZFS snapshot"""

    @property
    def prefix(self):
        (filesystem, snapshot) = self.name.split("@")
        return filesystem

    @property
    def suffix(self):
        (filesystem, snapshot_name) = self.name.split("@")
        return snapshot_name

    @property
    def typed_suffix(self):
        return "@" + self.suffix

    @property
    def tagless_suffix(self):
        """snapshot or bookmark part of the name, but without the tag. (if its our snapshot we remove the tag)"""

        if self.is_ours and self.zfs_node.tag_seperator in self.suffix:
            return self.suffix.split(self.zfs_node.tag_seperator)[0]
        else:
            return self.suffix

    @property
    def tag(self):
        """the tag-part of a snapshot or bookmark, if any. can return None"""

        if self.is_ours and self.zfs_node.tag_seperator in self.suffix:
            return self.suffix.split(self.zfs_node.tag_seperator)[1]
        else:
            return None

    @property
    def is_snapshot_excluded(self):
        """true if this dataset is a snapshot and matches the exclude pattern"""
        suffix = self.suffix
        for pattern in self.zfs_node.exclude_snapshot_patterns:
            if pattern.search(suffix) is not None:
                self.debug("Excluded (path matches snapshot exclude pattern)")
                return True

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
                cmd.extend(["-i", prev_snapshot.typed_suffix])

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

        cmd.append(self.prefix)

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

        self.debug("Transfer snapshot to {}".format(target_snapshot.prefix))

        if resume_token:
            self.verbose("resuming")

        # initial or increment
        if not prev_snapshot:
            self.verbose("-> {} (new)".format(target_snapshot.prefix))
        else:
            # incremental
            self.verbose("-> {}".format(target_snapshot.prefix))

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

    def bookmark(self, tag):
        """Bookmark this snapshot, and return the bookmark. If the bookmark already exist, it returns it."""
        # NOTE: we use the tag to add the target_path GUID, so that we can have multiple bookmarks for the same snapshot, but for different targets.
        # This is to make sure you can send a backup to two locations, without them interfering with eachothers bookmarks.

        if not self.is_snapshot:
            raise (Exception("Can only bookmark a snapshot!"))

        bookmark_name = self.prefix + "#" + self.tagless_suffix + self.zfs_node.tag_seperator + tag

        # does this exact bookmark (including tag) already exists?
        existing_bookmark = self.parent.find_exact_bookmark(bookmark_name)
        if existing_bookmark is not None:
            return existing_bookmark

        self.debug("Bookmarking {}".format(bookmark_name))

        cmd = [
            "zfs", "bookmark", self.name, bookmark_name
        ]

        self.zfs_node.run(cmd=cmd)

        bookmark = self.zfs_node.get_dataset(bookmark_name, force_exists=True)
        self.cache_snapshot_bookmark(bookmark)
        return bookmark

    @property
    def parent(self):
        """get parent dataset

        :rtype: ZfsContainer | None
        """
        return self.zfs_node.get_dataset(self.prefix)

    @property
    def holds(self):

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

        Note that the tag-part in the name is ignored, so snapsnots are ours regardless of their tag.

        :rtype: int|None
        """

        if self.zfs_node.tag_seperator in self.suffix:
            tagless_suffix = self.suffix.split(self.zfs_node.tag_seperator)[0]
        else:
            tagless_suffix = self.suffix

        try:
            dt = datetime.strptime(tagless_suffix, self.zfs_node.snapshot_time_format)
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

    def rollback(self):

        self.debug("Rolling back")
        self.zfs_node.run(["zfs", "rollback", self.name])

    def destroy(self, fail_exception=False, defered=False):

        self.verbose("Destroying")
        return super().destroy(fail_exception=fail_exception, deferred=defered)

    def clone(self, name):
        """clones this snapshot and returns ZfsDataset of the clone"""

        self.debug("Cloning to {}".format(name))

        cmd = [
            "zfs", "clone", self.name, name
        ]

        self.zfs_node.run(cmd=cmd, valid_exitcodes=[0])

        return self.zfs_node.get_dataset(name, force_exists=True)

    @property
    def is_ours(self):
        """return true if this snapshot name belong to the current backup_name and snapshot formatting"""
        return self.timestamp is not None
