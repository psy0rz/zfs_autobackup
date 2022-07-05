# python 2 compatibility
from __future__ import print_function
import re
import shlex
import subprocess
import sys
import time

from .ExecuteNode import ExecuteNode
from .Thinner import Thinner
from .CachedProperty import CachedProperty
from .ZfsPool import ZfsPool
from .ZfsDataset import ZfsDataset
from .ExecuteNode import ExecuteError


class ZfsNode(ExecuteNode):
    """a node that contains zfs datasets. implements global (systemwide/pool wide) zfs commands"""

    def __init__(self, logger, utc=False, snapshot_time_format="", hold_name="", ssh_config=None, ssh_to=None, readonly=False,
                 description="",
                 debug_output=False, thinner=None):

        self.utc = utc
        self.snapshot_time_format = snapshot_time_format
        self.hold_name = hold_name

        self.description = description

        self.logger = logger

        if ssh_config:
            self.verbose("Using custom SSH config: {}".format(ssh_config))

        if ssh_to:
            self.verbose("SSH to: {}".format(ssh_to))
        # else:
        #     self.verbose("Datasets are local")

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
        self.__datasets = {}

        self._progress_total_bytes = 0
        self._progress_start_time = time.time()

        ExecuteNode.__init__(self, ssh_config=ssh_config, ssh_to=ssh_to, readonly=readonly, debug_output=debug_output)

    def thin(self, objects, keep_objects):
        # NOTE: if thinning is disabled with --no-thinning, self.__thinner will be none.
        if self.__thinner is not None:
            return self.__thinner.thin(objects, keep_objects)
        else:
            return (keep_objects, [])

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

    def get_pool(self, dataset):
        """get a ZfsPool() object from dataset. stores objects internally to enable caching"""

        if not isinstance(dataset, ZfsDataset):
            raise (Exception("{} is not a ZfsDataset".format(dataset)))

        zpool_name = dataset.name.split("/")[0]

        return self.__pools.setdefault(zpool_name, ZfsPool(self, zpool_name))

    def get_dataset(self, name, force_exists=None):
        """get a ZfsDataset() object from name. stores objects internally to enable caching"""

        return self.__datasets.setdefault(name, ZfsDataset(self, name, force_exists))

    # def reset_progress(self):
    #     """reset progress output counters"""
    #     self._progress_total_bytes = 0
    #     self._progress_start_time = time.time()

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
                    # Reset the total bytes and start the timer again (otherwise the MB/s
                    # counter gets confused)
                    self._progress_total_bytes = int(progress_fields[2])
                    self._progress_start_time = time.time()
                elif progress_fields[0] == 'incremental':
                    # Reset the total bytes and start the timer again (otherwise the MB/s
                    # counter gets confused)
                    self._progress_total_bytes = int(progress_fields[3])
                    self._progress_start_time = time.time()
                elif progress_fields[1].isnumeric():
                    bytes_ = int(progress_fields[1])
                    if self._progress_total_bytes:
                        percentage = min(100, int(bytes_ * 100 / self._progress_total_bytes))
                        speed = int(bytes_ / (time.time() - self._progress_start_time) / (1024 * 1024))
                        bytes_left = self._progress_total_bytes - bytes_
                        minutes_left = int((bytes_left / (bytes_ / (time.time() - self._progress_start_time))) / 60)

                        self.logger.progress(
                            "Transfer {}% {}MB/s (total {}MB, {} minutes left)".format(percentage, speed, int(
                                self._progress_total_bytes / (1024 * 1024)), minutes_left))

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

    def warning(self, txt):
        self.logger.warning("{} {}".format(self.description, txt))

    def debug(self, txt):
        self.logger.debug("{} {}".format(self.description, txt))

    def consistent_snapshot(self, datasets, snapshot_name, min_changed_bytes, pre_snapshot_cmds=[],
                            post_snapshot_cmds=[], set_snapshot_properties=[]):
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
            snapshot = self.get_dataset(dataset.name + "@" + snapshot_name, force_exists=True)

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

        try:
            for cmd in pre_snapshot_cmds:
                self.verbose("Running pre-snapshot-cmd")
                self.run(cmd=shlex.split(cmd), readonly=False)

            # create consistent snapshot per pool
            for (pool_name, snapshots) in pools.items():
                cmd = ["zfs", "snapshot"]
                for snapshot_property in set_snapshot_properties:
                    cmd += ['-o', snapshot_property]

                cmd.extend(map(lambda snapshot_: str(snapshot_), snapshots))

                self.verbose("Creating snapshots {} in pool {}".format(snapshot_name, pool_name))
                self.run(cmd, readonly=False)

        finally:
            for cmd in post_snapshot_cmds:
                self.verbose("Running post-snapshot-cmd")
                try:
                    self.run(cmd=shlex.split(cmd), readonly=False)
                except Exception as e:
                    pass

    def selected_datasets(self, property_name, exclude_received, exclude_paths, exclude_unchanged, min_change):
        """determine filesystems that should be backed up by looking at the special autobackup-property, systemwide

           returns: list of ZfsDataset
        """

        self.debug("Getting selected datasets")

        # get all source filesystems that have the backup property
        lines = self.run(tab_split=True, readonly=True, cmd=[
            "zfs", "get", "-t", "volume,filesystem", "-o", "name,value,source", "-H",
            property_name
        ])

        # The returnlist of selected ZfsDataset's:
        selected_filesystems = []

        # list of sources, used to resolve inherited sources
        sources = {}

        for line in lines:
            (name, value, raw_source) = line
            dataset = self.get_dataset(name, force_exists=True)

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
            if dataset.is_selected(value=value, source=source, inherited=inherited, exclude_received=exclude_received,
                                   exclude_paths=exclude_paths, exclude_unchanged=exclude_unchanged,
                                   min_change=min_change):
                selected_filesystems.append(dataset)

        return selected_filesystems
