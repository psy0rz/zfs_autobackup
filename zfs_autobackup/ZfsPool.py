from zfs_autobackup.CachedProperty import CachedProperty


class ZfsPool():
    """a zfs pool"""

    def __init__(self, zfs_node, name):
        """name: name of the pool
        """

        self.zfs_node = zfs_node
        self.name = name

    def __repr__(self):
        return "{}: {}".format(self.zfs_node, self.name)

    def __str__(self):
        return self.name

    def __eq__(self, obj):
        if not isinstance(obj, ZfsPool):
            return False

        return self.name == obj.name

    def verbose(self, txt):
        self.zfs_node.verbose("zpool {}: {}".format(self.name, txt))

    def error(self, txt):
        self.zfs_node.error("zpool {}: {}".format(self.name, txt))

    def debug(self, txt):
        self.zfs_node.debug("zpool {}: {}".format(self.name, txt))

    @CachedProperty
    def properties(self):
        """all zpool properties"""

        self.debug("Getting zpool properties")

        cmd = [
            "zpool", "get", "-H", "-p", "all", self.name
        ]

        ret = {}

        for pair in self.zfs_node.run(tab_split=True, cmd=cmd, readonly=True, valid_exitcodes=[0]):
            ret[pair[1]] = pair[2]

        return ret

    @property
    def features(self):
        """get list of active zpool features"""

        ret = []
        for (key, value) in self.properties.items():
            if key.startswith("feature@"):
                feature = key.split("@")[1]
                if value == 'enabled' or value == 'active':
                    ret.append(feature)

        return ret