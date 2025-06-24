from .ExecuteNode import ExecuteError

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

        from .ZfsNode import ZfsNode

        self.zfs_node = zfs_node # type: ZfsNode
        self.name = name  # full actual name of dataset

        self.force_exists = force_exists

        # caching
        self.__exists_check = None  # type: None|bool
        self.__properties = None  # type: None|dict[str,str]

    def invalidate_cache(self):
        """clear caches"""
        self.force_exists = None
        self.__exists_check = None
        self.__properties = None

    def __repr__(self):
        return "{}: {}".format(self.zfs_node, self.name)

    def __str__(self):

        return self.name

    def __eq__(self, dataset):
        """compare the full name of the dataset"""

        if not isinstance(dataset, ZfsDataset):
            return False

        return self.name == dataset.name

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
    def exists_check(self):
        """check on disk if it exists"""

        if self.__exists_check is None:
            self.debug("Checking if dataset exists")
            self.__exists_check = (len(self.zfs_node.run(tab_split=True, cmd=["zfs", "list", self.name], readonly=True,
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
        self.__properties = None

    def inherit(self, prop):
        """inherit zfs property"""

        self.debug("Inheriting property {}".format(prop))

        cmd = [
            "zfs", "inherit", prop, self.name
        ]

        self.zfs_node.run(cmd=cmd, valid_exitcodes=[0])

        # invalidate cache
        self.__properties = None
