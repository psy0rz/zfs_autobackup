from .ExecuteNode import ExecuteError
import re
from datetime import datetime
import sys
import time

from .ZfsPointInTime import ZfsPointInTime


class ZfsBookmark(ZfsPointInTime):
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



    def destroy(self, fail_exception=False, **kwargs):

        self.debug("Destroying")
        return super().destroy(fail_exception=fail_exception)
