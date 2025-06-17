from .ZfsDataset import ZfsDataset
from .ExecuteNode import ExecuteError


class ZfsBookmark(ZfsDataset):
    """A ZFS bookmark"""

    @property
    def prefix(self):
        (filesystem, snapshot) = self.name.split("#")
        return filesystem

    @property
    def suffix(self):
        (filesystem, snapshot_name) = self.name.split("#")
        return snapshot_name

    @property
    def typed_suffix(self):
        return "#" + self.suffix

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
    def parent(self):
        """get parent dataset

        :rtype: ZfsContainer | None
        """
        return self.zfs_node.get_dataset(self.prefix)

    def destroy(self, fail_exception=False):

        self.debug("Destroying")
        return super().destroy(fail_exception=fail_exception)
