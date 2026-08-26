from typing import cast

from .ZfsBookmark import ZfsBookmark
from .ZfsDataset import ZfsDataset
from .ZfsPointInTime import ZfsPointInTime
from .ZfsSnapshot import ZfsSnapshot





class ZfsContainer(ZfsDataset):
    """Either a ZFS Filesystem or ZFS Dataset"""

    def __init__(self, zfs_node, name, force_exists=None):

        super().__init__(zfs_node, name, force_exists=force_exists)

        self._written_since_ours = None  # type: None|int
        self._recursive_datasets = None  # type: None|list[ZfsContainer]
        self._datasets = None  # type: None|list[ZfsContainer]
        self._snapshots_bookmarks = None  # type: None|list[ZfsSnapshot|ZfsBookmark]


    # def __str__(self):
    #
    #     return f"{self.zfs_node}: {self.name} {'force_exists' if self.force_exists else ''} {len(self._snapshots_bookmarks) if self._snapshots_bookmarks is not None else ''}"



    @property
    def parent(self):
        """get parent dataset

        :rtype: ZfsContainer | None
        """
        stripped = self.rstrip_path(1)
        if stripped:
            return self.zfs_node.get_container(stripped)
        else:
            return None

    def map_to_target_path(self, target_path, strip_path):
        """Map this dataset's name to a target path by stripping strip_path components and prepending target_path.
        :type target_path: str
        :type strip_path: int
        :rtype: str
        """
        stripped = self.lstrip_path(strip_path)
        if stripped:
            return target_path + "/" + stripped
        else:
            return target_path

    def invalidate_cache(self):
        super().invalidate_cache()
        self._written_since_ours = None
        self._recursive_datasets = None
        self._datasets = None
        self._snapshots_bookmarks = None

    @property
    def snapshots(self):
        """get all snapshots of this dataset
        :rtype: list[ZfsSnapshot]
        """
        ret = []

        for snapshot in self.snapshots_bookmarks:
            # check is snapshot is a ZfsSnapshot, and append to ret if it is
            if isinstance(snapshot, ZfsSnapshot):
                ret.append(snapshot)

        return ret

    @property
    def bookmarks(self):
        """get all bookmarks of this dataset
        Args:

            :rtype: list[ZfsBookmark]
        """

        ret = []

        for bookmark in self.snapshots_bookmarks:
            if type(bookmark) is ZfsBookmark:
                ret.append(bookmark)

        return ret

    @property
    def snapshots_bookmarks(self):
        """get all snapshots and bookmarks of this dataset (ordered by createtxg, so its suitable to determine incremental zfs send order)
        :rtype: list[ZfsSnapshot|ZfsBookmark]
        """

        # cached?
        if self._snapshots_bookmarks is None:
            self.debug("Getting snapshots and bookmarks")

            cmd = [
                "zfs", "list", "-d", "1", "-r", "-t", "snapshot,bookmark", "-H", "-o", "name", "-s", "createtxg",
                self.name
            ]

            names = cast("list[str]", self.zfs_node.run(cmd=cmd, readonly=True))
            self._snapshots_bookmarks = cast("list[ZfsSnapshot|ZfsBookmark]",
                                             self.zfs_node.get_datasets(names, force_exists=True))

        return self._snapshots_bookmarks

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

        if self.snapshots:
            if target_common_snapshot is None:
                # No common snapshot found: every existing target snapshot is in
                # the way and must be handled by handle_incompatible_snapshots.
                return list(self.snapshots)

            followup = True
            common_index = self.find_snapshot_index(target_common_snapshot)
            assert common_index is not None
            for snapshot in self.snapshots[common_index + 1:]:
                if raw or not followup or int(snapshot.properties['written']) != 0:
                    followup = False
                    ret.append(snapshot)

        return ret

    @property
    def written_since_ours(self):
        """get number of bytes written since our last snapshot
        :rtype: int
        """

        if self._written_since_ours is None:
            latest_snapshot = self.our_snapshots[-1]

            self.debug("Getting bytes written since our last snapshot")
            cmd = ["zfs", "get", "-H", "-ovalue", "-p", "written@" + str(latest_snapshot), self.name]

            output = cast("list[str]", self.zfs_node.run(readonly=True, tab_split=False, cmd=cmd, valid_exitcodes=[0]))

            self._written_since_ours = int(output[0])

        return self._written_since_ours

    def is_changed(self, min_changed_bytes=1):
        """dataset is changed since ANY latest snapshot ?

        Args:
            :type min_changed_bytes: int
        """
        self.debug("Checking if dataset is changed {} or more bytes".format(min_changed_bytes))

        if min_changed_bytes == 0:
            return True

        if int(self.properties['written']) < min_changed_bytes:
            return False
        else:
            return True

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
        ZfsSnapshot) Returns None if it can't find it.

        Note that matches with our own snapshots will be done tagless.

        Args:
            :rtype: ZfsSnapshot|None
            :type snapshot_name: str|ZfsPointInTime|None
        """

        if snapshot_name is None:
            return None

        if isinstance(snapshot_name, ZfsPointInTime):
            tagless_suffix = snapshot_name.tagless_suffix
        else:
            tagless_suffix = snapshot_name

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

        found = False
        for snapshot in self.snapshots_bookmarks:
            if snapshot == snapshot_bookmark:
                found = True
            else:
                if found and type(snapshot) is ZfsSnapshot:
                    return snapshot

        return None

    def automount(self):
        """Mount the dataset as if one did a zfs mount -a, but only for this dataset
        Failure to mount doesn't result in an exception, but outputs errors to STDERR.

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
        if not self.snapshots:
            #abort of an initial resume
            self.force_exists=False


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

        if self._recursive_datasets is None:
            self.debug("Getting all recursive datasets under us")

            names = cast("list[str]", self.zfs_node.run(tab_split=False, readonly=True, valid_exitcodes=[0], cmd=[
                "zfs", "list", "-r", "-t", types, "-o", "name", "-H", self.name
            ]))

            self._recursive_datasets = cast("list[ZfsContainer]",
                                            self.zfs_node.get_datasets(names[1:], force_exists=True))

        return self._recursive_datasets

    @property
    def datasets(self, types="filesystem,volume"):
        """get all (non-snapshot) datasets directly under us

        Args:
            :type types: str
            :rtype: list[ZfsContainer]

        """

        if self._datasets is None:
            self.debug("Getting all datasets under us")

            names = cast("list[str]", self.zfs_node.run(tab_split=False, readonly=True, valid_exitcodes=[0], cmd=[
                "zfs", "list", "-r", "-t", types, "-o", "name", "-H", "-d", "1", self.name
            ]))

            self._datasets = cast("list[ZfsContainer]",
                                  self.zfs_node.get_datasets(names[1:], force_exists=True))

        return self._datasets

    def create_filesystem(self, parents=False, unmountable=True):
        """create this container as a filesystem

        Args:
            :type parents: bool
            :type unmountable: bool
        """

        # recurse up
        parent = self.parent
        if parents and parent is not None and not parent.exists:
            parent.create_filesystem(parents, unmountable)

        cmd = ["zfs", "create"]

        if unmountable:
            cmd.extend(["-o", "canmount=off"])

        cmd.append(self.name)
        self.zfs_node.run(cmd)

        self.force_exists = True

        if self.zfs_node.readonly:
            self._snapshots_bookmarks=[]
            self.simulate_properties()

    def cache_snapshot_bookmark(self, snapshot, force=False):
        """Update our snapshot and bookmark cache (if we have any). Use force if you want to force the caching, potentially triggering a zfs list
        Args:
            :type snapshot: ZfsSnapshot|ZfsBookmark
            :type force: bool
        """

        if force:
            self.snapshots_bookmarks.append(snapshot)

        elif self._snapshots_bookmarks is not None:
            self._snapshots_bookmarks.append(snapshot)

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
        ZfsSnapshot or ZfsBookmark) Returns None if it can't find it.

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

        :rtype: ZfsBookmark|None
        """

        for snapshot_bookmark in self.bookmarks:
            if snapshot_bookmark.name == bookmark_name:
                return snapshot_bookmark
        return None

    def find_snapshot_index(self, snapshot):
        """find exact snapshot index by snapshot (can be a snapshot_name or
        ZfsSnapshot)

        Args:
            :type snapshot: str or ZfsSnapshot
            :rtype: int|None
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
            :rtype: tuple[ZfsSnapshot|ZfsBookmark, ZfsSnapshot] | tuple[None,None]
            :returns: (source_common_snapshot, target_common_snapshot)
            :type guid_check: bool
            :type target_dataset: ZfsContainer
            :type bookmark_tag: str
        """

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
                if guid_check and not source_snapshot.guid_matches(target_snapshot):
                    source_snapshot.warning("Snapshot has mismatching GUID, ignoring.")
                else:
                    source_snapshot.debug("Common snapshot")
                    return source_snapshot, target_snapshot

            # Extensive GUID search (slower but works with all names)
            source_bookmark_snapshot = self.find_guid_bookmark_snapshot(target_snapshot.properties['guid'])
            if source_bookmark_snapshot is not None:
                return source_bookmark_snapshot, target_snapshot

        return None, None

    def is_selected(self, value, source, inherited,  exclude_paths, exclude_unchanged):
        """determine if dataset should be selected for backup (called from
        ZfsNode)

        Args:
            :type exclude_paths: list[str]
            :type value: str
            :type source: str
            :type inherited: bool
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

        if not self.is_changed(exclude_unchanged):
            self.verbose("Excluded (by --exclude-unchanged)")
            return False

        self.verbose("Selected")
        return True

    def destroy(self, fail_exception=False, **kwargs):

        self.verbose("Destroying")
        return super().destroy(fail_exception=fail_exception)

    @property
    def origin(self):
        """The origin snapshot of this clone, or None if this dataset is not a clone or the origin cannot be parsed.

        :rtype: ZfsSnapshot|None
        """

        origin = self.properties.get('origin', '-')
        if origin == '-':
            return None

        if '@' not in origin:
            self.warning("Cannot parse clone origin '{}'.".format(origin))
            return None

        return self.zfs_node.get_snapshot(origin)
