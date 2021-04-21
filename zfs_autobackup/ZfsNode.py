# python 2 compatibility
from __future__ import print_function
import re
import subprocess
import sys
import time

from zfs_autobackup.ExecuteNode import ExecuteNode
from zfs_autobackup.Thinner import Thinner
from zfs_autobackup.CachedProperty import CachedProperty
from zfs_autobackup.ZfsPool import ZfsPool
from zfs_autobackup.ZfsDataset import ZfsDataset
from zfs_autobackup.ExecuteNode import ExecuteError


class ZfsNode(ExecuteNode):
    """a node that contains zfs datasets. implements global (systemwide/pool wide) zfs commands"""

    def __init__(self, backup_name, logger, ssh_config=None, ssh_to=None, readonly=False, description="",
                 debug_output=False, thinner=None):
        self.backup_name = backup_name
        self.description = description

        self.logger = logger

        if ssh_config:
            self.verbose("Using custom SSH config: {}".format(ssh_config))

        if ssh_to:
            self.verbose("Datasets on: {}".format(ssh_to))
        else:
            self.verbose("Datasets are local")

        if thinner is not None:
            rules = thinner.human_rules()
            if rules:
                for rule in rules:
                    self.verbose(rule)
            else:
                self.verbose("Keep no old snaphots")

        self.__thinner = thinner

        # list of ZfsPools
        self.__pools = {}

        self._progress_total_bytes = 0
        self._progress_start_time = time.time()

        ExecuteNode.__init__(self, ssh_config=ssh_config, ssh_to=ssh_to, readonly=readonly, debug_output=debug_output)

    def thin(self, objects, keep_objects):
        if self.__thinner is not None:
            return self.__thinner.thin(objects, keep_objects)
        else:
            return ( keep_objects, [] )

    @CachedProperty
    def supported_send_options(self):
        """list of supported options, for optimizing sends"""
        # not every zfs implementation supports them all

        ret = []
        for option in ["-L", "-e", "-c"]:
            if self.valid_command(["zfs", "send", option, "zfs_autobackup_option_test"]):
                ret.append(option)
        return ret

    @CachedProperty
    def supported_recv_options(self):
        """list of supported options"""
        # not every zfs implementation supports them all

        ret = []
        for option in ["-s"]:
            if self.valid_command(["zfs", "recv", option, "zfs_autobackup_option_test"]):
                ret.append(option)
        return ret

    def valid_command(self, cmd):
        """test if a specified zfs options are valid exit code. use this to determine support options"""

        try:
            self.run(cmd, hide_errors=True, valid_exitcodes=[0, 1])
        except ExecuteError:
            return False

        return True

    # TODO: also create a get_zfs_dataset() function that stores all the objects in a dict. This should optimize
    #  caching a bit and is more consistent.
    def get_zfs_pool(self, name):
        """get a ZfsPool() object from specified name. stores objects internally to enable caching"""

        return self.__pools.setdefault(name, ZfsPool(self, name))

    def reset_progress(self):
        """reset progress output counters"""
        self._progress_total_bytes = 0
        self._progress_start_time = time.time()

    def parse_zfs_progress(self, line, hide_errors, prefix):
        """try to parse progress output of zfs recv -Pv, and don't show it as error to the user """

        # is it progress output?
        progress_fields = line.rstrip().split("\t")

        if (line.find("nvlist version") == 0 or
                line.find("resume token contents") == 0 or
                len(progress_fields) != 1 or
                line.find("skipping ") == 0 or
                re.match("send from .*estimated size is ", line)):

            # always output for debugging offcourse
            self.debug(prefix + line.rstrip())

            # actual useful info
            if len(progress_fields) >= 3:
                if progress_fields[0] == 'full' or progress_fields[0] == 'size':
                    self._progress_total_bytes = int(progress_fields[2])
                elif progress_fields[0] == 'incremental':
                    self._progress_total_bytes = int(progress_fields[3])
                else:
                    bytes_ = int(progress_fields[1])
                    if self._progress_total_bytes:
                        percentage = min(100, int(bytes_ * 100 / self._progress_total_bytes))
                        speed = int(bytes_ / (time.time() - self._progress_start_time) / (1024 * 1024))
                        bytes_left = self._progress_total_bytes - bytes_
                        minutes_left = int((bytes_left / (bytes_ / (time.time() - self._progress_start_time))) / 60)

                        print(">>> {}% {}MB/s (total {}MB, {} minutes left)     \r".format(percentage, speed, int(
                            self._progress_total_bytes / (1024 * 1024)), minutes_left), end='', file=sys.stderr)
                        sys.stderr.flush()

            return

        # still do the normal stderr output handling
        if hide_errors:
            self.debug(prefix + line.rstrip())
        else:
            self.error(prefix + line.rstrip())

    # def _parse_stderr_pipe(self, line, hide_errors):
    #     self.parse_zfs_progress(line, hide_errors, "STDERR|> ")

    def _parse_stderr(self, line, hide_errors):
        self.parse_zfs_progress(line, hide_errors, "STDERR > ")

    def verbose(self, txt):
        self.logger.verbose("{} {}".format(self.description, txt))

    def error(self, txt):
        self.logger.error("{} {}".format(self.description, txt))

    def debug(self, txt):
        self.logger.debug("{} {}".format(self.description, txt))

    def new_snapshotname(self):
        """determine uniq new snapshotname"""
        return self.backup_name + "-" + time.strftime("%Y%m%d%H%M%S")

    def consistent_snapshot(self, datasets, snapshot_name, min_changed_bytes):
        """create a consistent (atomic) snapshot of specified datasets, per pool.
        """

        pools = {}

        # collect snapshots that we want to make, per pool
        # self.debug(datasets)
        for dataset in datasets:
            if not dataset.is_changed_ours(min_changed_bytes):
                dataset.verbose("No changes since {}".format(dataset.our_snapshots[-1].snapshot_name))
                continue

            # force_exist, since we're making it
            snapshot = ZfsDataset(dataset.zfs_node, dataset.name + "@" + snapshot_name, force_exists=True)

            pool = dataset.split_path()[0]
            if pool not in pools:
                pools[pool] = []

            pools[pool].append(snapshot)

            # update cache, but try to prevent an unneeded zfs list
            if self.readonly or CachedProperty.is_cached(dataset, 'snapshots'):
                dataset.snapshots.append(snapshot)  # NOTE: this will trigger zfs list if its not cached

        if not pools:
            self.verbose("No changes anywhere: not creating snapshots.")
            return

        # create consistent snapshot per pool
        for (pool_name, snapshots) in pools.items():
            cmd = ["zfs", "snapshot"]

            cmd.extend(map(lambda snapshot_: str(snapshot_), snapshots))

            self.verbose("Creating snapshots {} in pool {}".format(snapshot_name, pool_name))
            self.run(cmd, readonly=False)

    @CachedProperty
    def selected_datasets(self, ignore_received=True):
        """determine filesystems that should be backupped by looking at the special autobackup-property, systemwide

           returns: list of ZfsDataset
        """

        self.debug("Getting selected datasets")

        # get all source filesystems that have the backup property
        lines = self.run(tab_split=True, readonly=True, cmd=[
            "zfs", "get", "-t", "volume,filesystem", "-o", "name,value,source", "-H",
            "autobackup:" + self.backup_name
        ])

        # The returnlist of selected ZfsDataset's:
        selected_filesystems = []

        # list of sources, used to resolve inherited sources
        sources = {}

        for line in lines:
            (name, value, raw_source) = line
            dataset = ZfsDataset(self, name)

            # "resolve" inherited sources
            sources[name] = raw_source
            if raw_source.find("inherited from ") == 0:
                inherited = True
                inherited_from = re.sub("^inherited from ", "", raw_source)
                source = sources[inherited_from]
            else:
                inherited = False
                source = raw_source

            # determine it
            if dataset.is_selected(value=value, source=source, inherited=inherited, ignore_received=ignore_received):
                selected_filesystems.append(dataset)

        return selected_filesystems
