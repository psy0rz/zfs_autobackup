from typing import TYPE_CHECKING, cast

from .ExecuteNode import ExecuteError

if TYPE_CHECKING:
    from .ZfsNode import ZfsNode

#
# Inheritance / overview graph (comment):
#
# ZfsDataset
# ├─ ZfsPointInTime (zfs_autobackup/ZfsPointInTime.py)
# │  ├─ ZfsSnapshot   (zfs_autobackup/ZfsSnapshot.py)
# │  └─ ZfsBookmark   (zfs_autobackup/ZfsBookmark.py)
# └─ ZfsContainer     (zfs_autobackup/ZfsContainer.py)
#
# Summary (key responsibilities / notable members):
# - ZfsDataset (this file)
#   - Base for all dataset types (filesystems, volumes, snapshots, bookmarks)
#   - Key attributes: .zfs_node, .name, .force_exists
#   - Important properties: .exists_check, .exists, .properties
#   - Important methods: destroy(), invalidate_cache(), set(), inherit(), mount(), unmount()
# - ZfsPointInTime
#   - Abstract intermediate for point-in-time objects (snapshots & bookmarks)
#   - Key: .prefix, .suffix, .timestamp, .is_ours, .tagless_suffix, .tag
# - ZfsSnapshot
#   - Snapshot-specific operations: send_pipe(), recv_pipe(), transfer_snapshot(), bookmark(), clone(), hold/release
# - ZfsBookmark
#   - Bookmark-specific behavior (name split on "#", light destroy override)
# - ZfsContainer
#   - Filesystem/volume container logic: snapshots/bookmarks management, automount, thin/thin_list, sync_snapshots, find_common_snapshot
##


class ZfsDataset:
    """A generic ZFS dataset, this has all the common functions of zfs filesystems, volumes, snapshots and bookmarks.

    It is used as a base class for ZfsSnapshot, ZfsBookmark and ZfsContainer. This class should not be instantiated directly.
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

        # prevent direct instantiation
        if type(self) is ZfsDataset:
            raise TypeError(
                "should not be instantiated directly.")

        self.zfs_node = zfs_node  # type: ZfsNode
        self.name = name  # full actual name of dataset

        self.force_exists = force_exists

        # caching
        self._exists_check = None  # type: None|bool
        self._properties = None  # type: None|dict[str,str]

    def invalidate_cache(self):
        """clear caches"""
        self.force_exists = None
        self._exists_check = None
        self._properties = None

    def __repr__(self):
        return "{}: {}".format(self.zfs_node, self.name)

    def __str__(self):

        # return f"{self.zfs_node}: {self.name} {'force_exists' if self.force_exists else ''}"
        return self.name

    def __eq__(self, dataset):
        """compare the full name of the dataset"""

        if not isinstance(dataset, ZfsDataset):
            return False

        return self.name == dataset.name

    def __hash__(self):
        return hash(self.name)

    def guid_matches(self, other):
        """Compare GUIDs between this dataset and other. Returns True if they match,
        or if either side has an empty guid (test mode simulation — can't compare).

        :type other: ZfsDataset
        :rtype: bool
        """
        guid_self = self.properties.get('guid', '')
        guid_other = other.properties.get('guid', '')
        if guid_self == '' or guid_other == '':
            return True
        return guid_self == guid_other

    def verbose(self, txt):
        """
        Args:
            :type txt: str
        """
        self.zfs_node.verbose("{}: {}".format(self, txt))

    def error(self, txt):
        """
        Args:
            :type txt: str
        """
        self.zfs_node.error("{}: {}".format(self, txt))

    def warning(self, txt):
        """
        Args:
            :type txt: str
        """
        self.zfs_node.warning("{}: {}".format(self, txt))

    def debug(self, txt):
        """
        Args:
            :type txt: str
        """
        self.zfs_node.debug("{}: {}".format(self, txt))

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
    def exists_check(self):
        """check on disk if it exists"""

        if self._exists_check is None:
            self.debug("Checking if dataset exists")
            output = cast("list[str]", self.zfs_node.run(
                tab_split=True, cmd=["zfs", "list", self.name], readonly=True,
                valid_exitcodes=[0, 1], hide_errors=True))
            self._exists_check = (len(output) > 0)

        return self._exists_check

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

    def destroy(self, fail_exception=False, deferred=False):
        """destroy the dataset. by default failures are not an exception, so we
        can continue making backups

        Args:
            :type deferred: bool
            :type fail_exception: bool
        """

        try:
            if deferred:
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
        """all zfs properties
        :rtype: dict[str, str]
        """

        if self._properties is None:

            cmd = [
                "zfs", "get", "-H", "-o", "property,value", "-p", "all", self.name
            ]

            self.debug("Getting zfs properties")
            properties = {}  # type: dict[str, str]

            output = cast("list[list[str]]", cast(object, self.zfs_node.run(
                tab_split=True, cmd=cmd, readonly=True, valid_exitcodes=[0])))
            for pair in output:
                if len(pair) == 2:
                    properties[pair[0]] = pair[1]
            self._properties = properties

        return self._properties


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

    def set(self, prop, value):
        """set a zfs property"""

        self.debug("Setting {}={}".format(prop, value))

        cmd = [
            "zfs", "set", "{}={}".format(prop, value), self.name
        ]

        self.zfs_node.run(cmd=cmd, valid_exitcodes=[0])

        # invalidate cache
        self._properties = None

    def inherit(self, prop):
        """inherit zfs property"""

        self.debug("Inheriting property {}".format(prop))

        cmd = [
            "zfs", "inherit", prop, self.name
        ]

        self.zfs_node.run(cmd=cmd, valid_exitcodes=[0])

        # invalidate cache
        self._properties = None

    def mount(self, mount_point):
        """Mount the container or snapshot at mount_point, if it is a filesystem."""

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

    def simulate_properties(self):
        """in test mode when we create fake snapshots or bookmarks, we also need to simluate properties"""
        if self._properties is None:
            self._properties={
                'guid': ''
            }
