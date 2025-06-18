from .ZfsBookmark import ZfsBookmark
from .ZfsDataset import ZfsDataset
from .ExecuteNode import ExecuteError
from .ZfsSnapshot import ZfsSnapshot
import re
from datetime import datetime
import sys
import time


class ZfsContainer(ZfsDataset):
    """Either a ZFS Filesystem or ZFS Dataset"""

    def __init__(self, zfs_node, name, force_exists=None):

        super().__init__(zfs_node, name, force_exists=force_exists)

        self.__written_since_ours = None  # type: None|int
        self.__recursive_datasets = None  # type: None|list[ZfsContainer]
        self.__datasets = None  # type: None|list[ZfsContainer]
        self.__snapshots_bookmarks = None  # type: None|list[ZfsSnapshot|ZfsBookmark]

    @property
    def parent(self):
        """get parent dataset

        :rtype: ZfsContainer | None
        """
        stripped = self.rstrip_path(1)
        if stripped:
            return self.zfs_node.get_dataset(stripped)
        else:
            return None


    def invalidate_cache(self):
        super().invalidate_cache()
        self.__written_since_ours = None
        self.__recursive_datasets = None
        self.__datasets = None
        self.__snapshots_bookmarks = None

    @property
    def snapshots(self):
        """get all snapshots of this dataset
        :rtype: list[ZfsSnapshot]
        """
        ret = []

        for snapshot in self.snapshots_bookmarks:
            if snapshot is ZfsSnapshot:
                ret.append(snapshot)

        return ret

    def find_incompatible_snapshots(self, target_common_snapshot, raw):
        """returns a list[snapshots] that is incompatible for a zfs recv onto
        the common_snapshot. all direct followup snapshots with written=0 are
        compatible.

        in raw-mode nothing is compatible. issue #219

        Args:
            :type target_common_snapshot: ZfsSnapshot
            :type raw: bool
        """

        ret = []

        if target_common_snapshot and self.snapshots:
            followup = True
            for snapshot in self.snapshots[self.find_snapshot_index(target_common_snapshot) + 1:]:
                if raw or not followup or int(snapshot.properties['written']) != 0:
                    followup = False
                    ret.append(snapshot)

        return ret

    @property
    def written_since_ours(self):
        """get number of bytes written since our last snapshot"""

        if self.__written_since_ours is None:
            latest_snapshot = self.our_snapshots[-1]

            self.debug("Getting bytes written since our last snapshot")
            cmd = ["zfs", "get", "-H", "-ovalue", "-p", "written@" + str(latest_snapshot), self.name]

            output = self.zfs_node.run(readonly=True, tab_split=False, cmd=cmd, valid_exitcodes=[0])

            self.__written_since_ours = int(output[0])

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

    def find_snapshot(self, snapshot_name):
        """find snapshot by snapshot name (can be a suffix or a different
        ZfsSnapshot) Returns None if it cant find it.

        Note that matches with our own snapshots will be done tagless.

        Args:
            :rtype: ZfsSnapshot|None
            :type snapshot_name: str|ZfsSnapshot|None
        """

        if snapshot_name is None:
            return None

        if not isinstance(snapshot_name, ZfsDataset):
            tagless_suffix = snapshot_name
        else:
            tagless_suffix = snapshot_name.tagless_suffix

        for snapshot_name in self.snapshots:
            if snapshot_name.tagless_suffix == tagless_suffix:
                return snapshot_name

        return None

    def find_next_snapshot(self, snapshot_bookmark):
        """find next snapshot in this dataset, according to snapshot or bookmark. None if it doesn't exist
        Args:
            :type snapshot_bookmark: ZfsSnapshot|ZfsBookmark
            :rtype: ZfsSnapshot|None

        """

        if not self.is_dataset:
            raise (Exception("Please call this on a dataset."))

        found = False
        for snapshot in self.snapshots_bookmarks:
            if snapshot == snapshot_bookmark:
                found = True
            else:
                if found and snapshot is ZfsSnapshot:
                    return snapshot

        return None

    def mount(self, mount_point):
        """Mount the dataset at mount_point, if it is a filesystem."""

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

    @property
    def bookmarks(self):
        """get all bookmarks of this dataset
        Args:

            :rtype: list[ZfsBookmark]
        """

        ret = []

        for bookmark in self.snapshots_bookmarks:
            if bookmark is ZfsBookmark:
                ret.append(bookmark)

        return ret

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

        self.zfs_node.run(["zfs", "mount", self.name], valid_exitcodes=[0, 1])

    def abort_resume(self):
        """abort current resume state"""
        self.debug("Aborting resume")
        self.zfs_node.run(["zfs", "recv", "-A", self.name])

    def rollback(self):
        """rollback to latest existing snapshot on this dataset"""

        for snapshot in reversed(self.snapshots):
            if snapshot.exists:
                snapshot.rollback()
                return

    def thin_list(self, keeps=None, ignores=None):
        """determines list[snapshots] that should be kept or deleted based on
        the thinning schedule. cull the herd!

        returns: ( keeps, obsoletes )

        Args:
            :param keeps: list[snapshots] to always keep (usually the last)
            :param ignores: snapshots to completely ignore (usually incompatible target snapshots that are going to be destroyed anyway)
            :type keeps: list[ZfsSnapshot]
            :type ignores: list[ZfsSnapshot]
            :rtype: tuple[list[ZfsSnapshot], list[ZfsSnapshot]]
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

    @property
    def recursive_datasets(self, types="filesystem,volume"):
        """get all (non-snapshot) datasets recursively under us

        Args:
            :type types: str
            :rtype: list[ZfsContainer]
        """

        if self.__recursive_datasets is None:
            self.debug("Getting all recursive datasets under us")

            names = self.zfs_node.run(tab_split=False, readonly=True, valid_exitcodes=[0], cmd=[
                "zfs", "list", "-r", "-t", types, "-o", "name", "-H", self.name
            ])

            self.__recursive_datasets = self.zfs_node.get_datasets(names[1:], force_exists=True)

        return self.__recursive_datasets

    @property
    def datasets(self, types="filesystem,volume"):
        """get all (non-snapshot) datasets directly under us

        Args:
            :type types: str
            :rtype: list[ZfsContainer]

        """

        if self.__datasets is None:
            self.debug("Getting all datasets under us")

            names = self.zfs_node.run(tab_split=False, readonly=True, valid_exitcodes=[0], cmd=[
                "zfs", "list", "-r", "-t", types, "-o", "name", "-H", "-d", "1", self.name
            ])

            self.__datasets = self.zfs_node.get_datasets(names[1:], force_exists=True)

        return self.__datasets


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

    @property
    def snapshots_bookmarks(self):
        """get all snapshots and bookmarks of this dataset (ordered by createtxg, so its suitable to determine incremental zfs send order)
        :rtype: list[ZfsSnapshot|ZfsBookmark]
        """

        # cached?
        if self.__snapshots_bookmarks is None:
            self.debug("Getting snapshots and bookmarks")

            cmd = [
                "zfs", "list", "-d", "1", "-r", "-t", "snapshot,bookmark", "-H", "-o", "name", "-s", "createtxg",
                self.name
            ]

            self.__snapshots_bookmarks = self.zfs_node.get_datasets(self.zfs_node.run(cmd=cmd, readonly=True),
                                                                    force_exists=True)

        return self.__snapshots_bookmarks

    def cache_snapshot_bookmark(self, snapshot, force=False):
        """Update our snapshot and bookmark cache (if we have any). Use force if you want to force the caching, potentially triggering a zfs list
        Args:
            :type snapshot: ZfsSnapshot|ZfsBookmark
        """

        if force:
            self.snapshots_bookmarks.append(snapshot)

        elif self.__snapshots_bookmarks is not None:
            self.__snapshots_bookmarks.append(snapshot)

    @property
    def our_snapshots(self):
        """get list[snapshots] creates by us of this dataset"""
        ret = []

        for snapshot in self.snapshots:
            if snapshot.is_ours:
                ret.append(snapshot)

        return ret


    def find_guid_bookmark_snapshot(self, guid):
        """find the first bookmark or snapshot that matches, prefers bookmarks.
            Args:
            :type guid:str
            :rtype: ZfsBookmark|ZfsSnapshot|None
        """
        # Since this is slower, we only use it if the name matching with find_snapshot and find_bookmark doesn work.

        for bookmark in self.bookmarks:
            if bookmark.properties['guid'] == guid:
                return bookmark

        for snapshot in self.snapshots:
            if snapshot.properties['guid'] == guid:
                return snapshot

        return None

    def find_bookmark(self, snapshot_bookmark, preferred_tag):
        """find bookmark by bookmark name (can be a suffix or a different
        ZfsDataset) Returns None if it cant find it.

        We try to find the bookmark with the preferred tag (which is usually a target path guid, to prevent conflicting bookmarks by multiple sends)
        If that fails, we return any bookmark that matches (and ignore the tag)

        Args:
            :rtype: ZfsBookmark|None
            :type snapshot_bookmark: str|ZfsBookmark|ZfsSnapshot|None
            :type preferred_tag: str
        """

        if snapshot_bookmark is None:
            return None

        if not isinstance(snapshot_bookmark, ZfsDataset):
            tagless_suffix = snapshot_bookmark
        else:
            tagless_suffix = snapshot_bookmark.tagless_suffix

        for snapshot_bookmark in self.bookmarks:
            if snapshot_bookmark.tagless_suffix == tagless_suffix and snapshot_bookmark.tag == preferred_tag:
                return snapshot_bookmark

        for snapshot_bookmark in self.bookmarks:
            if snapshot_bookmark.tagless_suffix == tagless_suffix:
                return snapshot_bookmark

        return None

    def find_exact_bookmark(self, bookmark_name):
        """find exact bookmark name, or retruns none

        :rtype: ZfsDataset|None
        """

        for snapshot_bookmark in self.bookmarks:
            if snapshot_bookmark.name == bookmark_name:
                return snapshot_bookmark
        return None

    def find_snapshot_index(self, snapshot):
        """find exact snapshot index by snapshot (can be a snapshot_name or
        ZfsDataset)

        Args:
            :type snapshot: str or ZfsDataset
        """

        if not isinstance(snapshot, ZfsDataset):
            snapshot_name = snapshot
        else:
            snapshot_name = snapshot.suffix

        index = 0
        for snapshot in self.snapshots:
            if snapshot.suffix == snapshot_name:
                return index
            index = index + 1

        return None

    def find_common_snapshot(self, target_dataset, guid_check, bookmark_tag):
        """find latest common snapshot/bookmark between us and target returns None if its
        an initial transfer.

        On the source it prefers the specified bookmark_name

        Args:
            :rtype: tuple[ZfsDataset, ZfsDataset] | tuple[None,None]
            :type guid_check: bool
            :type target_dataset: ZfsDataset
            :type bookmark_tag: str
        """

        if not target_dataset.exists or not target_dataset.snapshots:
            # target has nothing yet
            return None, None
        else:
            for target_snapshot in reversed(target_dataset.snapshots):

                # Prefer bookmarks to snapshots
                source_bookmark = self.find_bookmark(target_snapshot, preferred_tag=bookmark_tag)
                if source_bookmark:
                    if guid_check and source_bookmark.properties['guid'] != target_snapshot.properties['guid']:
                        source_bookmark.warning("Bookmark has mismatching GUID, ignoring.")
                    else:
                        source_bookmark.debug("Common bookmark")
                        return source_bookmark, target_snapshot

                # Source snapshot with same suffix?
                source_snapshot = self.find_snapshot(target_snapshot)
                if source_snapshot:
                    if guid_check and source_snapshot.properties['guid'] != target_snapshot.properties['guid']:
                        source_snapshot.warning("Snapshot has mismatching GUID, ignoring.")
                    else:
                        source_snapshot.debug("Common snapshot")
                        return source_snapshot, target_snapshot

                # Extensive GUID search (slower but works with all names)
                try:
                    source_bookmark_snapshot = self.find_guid_bookmark_snapshot(target_snapshot.properties['guid'])
                    if source_bookmark_snapshot is not None:
                        return source_bookmark_snapshot, target_snapshot
                except ExecuteError as e:
                    # in readonly mode we igore a failed property read for non existing snapshots
                    if not self.zfs_node.readonly:
                        raise e

            raise (Exception("Cant find common bookmark or snapshot with target."))

    def _pre_clean(self, source_common_snapshot, target_dataset, source_obsoletes, target_obsoletes, target_transfers):
        """cleanup old stuff before starting snapshot syncing

        Args:
            :type source_common_snapshot: ZfsDataset
            :type target_dataset: ZfsDataset
            :type source_obsoletes: list[ZfsDataset]
            :type target_obsoletes: list[ZfsDataset]
            :type target_transfers: list[ZfsDataset]
        """

        # on source: delete all obsoletes that are not in target_transfers (except common snapshot, if its not a bookmark)
        for source_snapshot in self.snapshots:
            if (source_snapshot in source_obsoletes
                    and source_common_snapshot != source_snapshot
                    and source_snapshot.find_snapshot_in_list(target_transfers) is None):
                source_snapshot.destroy()

        # on target: destroy everything thats obsolete, except common_snapshot
        if target_dataset.exists:
            for target_snapshot in target_dataset.snapshots:
                if (target_snapshot in target_obsoletes) \
                        and (not source_common_snapshot or (
                        target_snapshot.tagless_suffix != source_common_snapshot.tagless_suffix)):
                    if target_snapshot.exists:
                        target_snapshot.destroy()

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

    def destroy(self, fail_exception=False):

        self.verbose("Destroying")
        return super().destroy(fail_exception=fail_exception)

    def sync_snapshots(self, target_dataset, features, show_progress, filter_properties, set_properties,
                       ignore_recv_exit_code, holds, rollback, decrypt, encrypt, also_other_snapshots,
                       no_send, destroy_incompatible, send_pipes, recv_pipes, zfs_compressed, force, guid_check,
                       use_bookmarks, bookmark_tag):
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
            :type use_bookmarks: bool
            :type bookmark_tag: str
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

        (source_common_snapshot, source_obsoletes, target_obsoletes, target_transfers,
         incompatible_target_snapshots) = \
            self._plan_sync(target_dataset=target_dataset, also_other_snapshots=also_other_snapshots,
                            guid_check=guid_check, raw=raw, bookmark_tag=bookmark_tag)

        # NOTE: we do a pre-clean because we dont want filesystems to fillup when backups keep failing.
        # Also usefull with no_send to still cleanup stuff.
        self._pre_clean(
            source_common_snapshot=source_common_snapshot, target_dataset=target_dataset,
            target_transfers=target_transfers, target_obsoletes=target_obsoletes, source_obsoletes=source_obsoletes)

        # handle incompatible stuff on target
        target_dataset.handle_incompatible_snapshots(incompatible_target_snapshots, destroy_incompatible)

        # now actually transfer the snapshots, if we want
        if no_send or len(target_transfers) == 0:
            return

        # check if we can resume
        resume_token = self._validate_resume_token(target_dataset, target_transfers[0])

        (active_filter_properties, active_set_properties) = self.get_allowed_properties(filter_properties,
                                                                                        set_properties)

        # always filter properties that start with 'autobackup:' (https://github.com/psy0rz/zfs_autobackup/issues/221)
        for property in self.properties:
            if property.startswith('autobackup:'):
                active_filter_properties.append(property)

        # encrypt at target?
        if encrypt and not raw:
            # filter out encryption properties to let encryption on the target take place
            active_filter_properties.extend(["keylocation", "pbkdf2iters", "keyformat", "encryption"])
            write_embedded = False

        # now actually transfer the snapshots
        do_rollback = rollback
        prev_source_snapshot_bookmark = source_common_snapshot
        prev_target_snapshot = target_dataset.find_snapshot(source_common_snapshot)
        for target_snapshot in target_transfers:

            source_snapshot = self.find_snapshot(target_snapshot)

            # do the rollback, one time at first transfer
            if do_rollback:
                target_dataset.rollback()
                do_rollback = False

            source_snapshot.transfer_snapshot(target_snapshot, features=features,
                                              prev_snapshot=prev_source_snapshot_bookmark, show_progress=show_progress,
                                              filter_properties=active_filter_properties,
                                              set_properties=active_set_properties,
                                              ignore_recv_exit_code=ignore_recv_exit_code,
                                              resume_token=resume_token, write_embedded=write_embedded, raw=raw,
                                              send_properties=send_properties, send_pipes=send_pipes,
                                              recv_pipes=recv_pipes, zfs_compressed=zfs_compressed, force=force)

            resume_token = None

            # hold/release common snapshot on the target.
            if holds:
                target_snapshot.hold()

                if prev_target_snapshot:
                    prev_target_snapshot.release()

            if use_bookmarks:
                # bookmark common snapshot, and clean up obsolete snapshots and bookmark
                source_bookmark = source_snapshot.bookmark(bookmark_tag)
                if source_snapshot in source_obsoletes:
                    source_snapshot.destroy()

                # TODO: make a better is_ours specially for bookmarks, as part of the next refactoring splitting in more classes
                # delete any bookmark that ends in ours tag_seprator + tag.
                if prev_source_snapshot_bookmark and prev_source_snapshot_bookmark.is_bookmark and prev_source_snapshot_bookmark.name.endswith(
                        self.zfs_node.tag_seperator + bookmark_tag):
                    prev_source_snapshot_bookmark.destroy()


            # dont use bookmarks
            else:
                source_bookmark = None
                if holds:
                    source_snapshot.hold()

                # release hold, cleanup obsolete snapshot
                if prev_source_snapshot_bookmark and prev_source_snapshot_bookmark.is_snapshot:
                    prev_source_snapshot_bookmark.release()
                    if prev_source_snapshot_bookmark in source_obsoletes:
                        prev_source_snapshot_bookmark.destroy()

            # destroy the previous target snapshot if obsolete (usually this is only the common_snapshot,
            # the rest was already destroyed or will not be send)
            if prev_target_snapshot in target_obsoletes:
                prev_target_snapshot.destroy()

            # we always try to use the bookmark during incremental send
            if source_bookmark:
                prev_source_snapshot_bookmark = source_bookmark
            else:
                prev_source_snapshot_bookmark = source_snapshot

            prev_target_snapshot = target_snapshot

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
                    ### XXX: hacky, maybe not do it and have things check snapshot.exists instead?
                    self.snapshots.remove(snapshot)

                if len(incompatible_target_snapshots) > 0:
                    self.rollback()

    def _plan_sync(self, target_dataset, also_other_snapshots, guid_check, raw, bookmark_tag):
        """Determine at what snapshot to start syncing to target_dataset and what to sync and what to keep.

        Args:
            :rtype: ( ZfsDataset,  list[ZfsDataset], list[ZfsDataset], list[ZfsDataset], list[ZfsDataset] )
            :type target_dataset: ZfsDataset
            :type also_other_snapshots: bool
            :type guid_check: bool
            :type raw: bool
            :type bookmark_tag: str

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
        (source_common_snapshot, target_common_snapshot) = self.find_common_snapshot(target_dataset,
                                                                                     guid_check=guid_check,
                                                                                     bookmark_tag=bookmark_tag)
        # if source_common_snapshot:
        #     source_common_snapshot.verbose("Common snapshot or bookmark")
        incompatible_target_snapshots = target_dataset.find_incompatible_snapshots(target_common_snapshot, raw)

        # let thinner decide whats obsolete on source after the transfer is done
        source_obsoletes = []
        if self.our_snapshots:
            source_obsoletes = self.thin_list()[1]

        ### 2: Determine possible target snapshots

        # start with snapshots that already exist, minus imcompatibles
        if target_dataset.exists:
            possible_target_snapshots = [snapshot for snapshot in target_dataset.snapshots if
                                         snapshot not in incompatible_target_snapshots]
        else:
            possible_target_snapshots = []

        # add all snapshots from the source, starting after the common snapshot if it exists
        if source_common_snapshot:
            source_snapshot = self.find_next_snapshot(source_common_snapshot)
        else:
            if self.snapshots:
                source_snapshot = self.snapshots[0]
            else:
                source_snapshot = None

        while source_snapshot:
            # we want it?
            if (also_other_snapshots or source_snapshot.is_ours) and not source_snapshot.is_snapshot_excluded:
                # create virtual target snapshot
                target_snapshot = target_dataset.zfs_node.get_dataset(
                    target_dataset.prefix + source_snapshot.typed_suffix, force_exists=False)
                possible_target_snapshots.append(target_snapshot)
            source_snapshot = self.find_next_snapshot(source_snapshot)

        ### 3: Let the thinner decide what it wants by looking at all the possible target_snaphots at once.
        # always keep the last target snapshot as common snapshot.
        if possible_target_snapshots:
            (target_keeps, target_obsoletes) = target_dataset.zfs_node.thin_list(possible_target_snapshots,
                                                                                 keep_snapshots=[
                                                                                     possible_target_snapshots[-1]])
        else:
            target_keeps = []
            target_obsoletes = []

        ### 4: Look at what the thinner wants and create a list of snapshots we still need to transfer
        target_transfers = []
        for target_keep in target_keeps:
            if not target_keep.exists:
                target_transfers.append(target_keep)

        return source_common_snapshot, source_obsoletes, target_obsoletes, target_transfers, incompatible_target_snapshots

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
                # not valid anymore?
                resume_snapshot = self.get_resume_snapshot(resume_token)
                if not resume_snapshot or start_snapshot.suffix != resume_snapshot.suffix:
                    target_dataset.verbose("Aborting resume, its no longer valid.")
                    target_dataset.abort_resume()
                else:
                    return resume_token
