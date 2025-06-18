from zfs_autobackup.ZfsDataset import ZfsDataset
from datetime import datetime
import time


class ZfsPointInTime(ZfsDataset):
    """contains stuff that is common to snapshots and bookmarks"""


    #implemented in subclass
    @property
    def suffix(self):
        return ""

    #implemented in subclass
    @property
    def prefix(self):
        return ""


    @property
    def parent(self):
        """get parent dataset

        :rtype: ZfsContainer | None
        """
        return self.zfs_node.get_dataset(self.prefix)


    @property
    def timestamp(self):
        """get timestamp from snapshot/bookmark name. Only works for our own snapshots
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

    @property
    def is_ours(self):
        """return true if this snapshot name belong to the current backup_name and snapshot formatting"""
        return self.timestamp is not None


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

