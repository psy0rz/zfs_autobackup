import time

from zfs_autobackup.ThinnerRule import ThinnerRule


class Thinner:
    """progressive thinner (universal, used for cleaning up snapshots)"""

    def __init__(self, schedule_str=""):
        """
        Args:
            schedule_str: comma seperated list of ThinnerRules. A plain number specifies how many snapshots to always keep.
        """

        self.rules = []
        self.always_keep = 0

        if schedule_str == "":
            return

        rule_strs = schedule_str.split(",")
        for rule_str in rule_strs:
            if rule_str.lstrip('-').isdigit():
                self.always_keep = int(rule_str)
                if self.always_keep < 0:
                    raise (Exception("Number of snapshots to keep cant be negative: {}".format(self.always_keep)))
            else:
                self.rules.append(ThinnerRule(rule_str))

    def human_rules(self):
        """get list of human readable rules"""
        ret = []
        if self.always_keep:
            ret.append("Keep the last {} snapshot{}.".format(self.always_keep, self.always_keep != 1 and "s" or ""))
        for rule in self.rules:
            ret.append(rule.human_str)

        return ret

    def thin(self, objects, keep_objects=None, now=None):
        """thin list of objects with current schedule rules. objects: list of
        objects to thin. every object should have timestamp attribute.

            return( keeps, removes )

        Args:
            objects: list of objects to check (should have a timestamp attribute)
            keep_objects: objects to always keep (if they also are in the in the normal objects list)
            now: if specified, use this time as current time
        """

        if not keep_objects:
            keep_objects = []

        # always keep a number of the last objets?
        if self.always_keep:
            # all of them
            if len(objects) <= self.always_keep:
                return objects, []

            # determine which ones
            always_keep_objects = objects[-self.always_keep:]
        else:
            always_keep_objects = []

        # determine time blocks
        time_blocks = {}
        for rule in self.rules:
            time_blocks[rule.period] = {}

        if not now:
            now = int(time.time())

        keeps = []
        removes = []

        # traverse objects
        for thisobject in objects:
            # important they are ints!
            timestamp = int(thisobject.timestamp)
            age = int(now) - timestamp

            # store in the correct time blocks, per period-size, if not too old yet
            # e.g.: look if there is ANY timeblock that wants to keep this object
            keep = False
            for rule in self.rules:
                if age <= rule.ttl:
                    block_nr = int(timestamp / rule.period)
                    if block_nr not in time_blocks[rule.period]:
                        time_blocks[rule.period][block_nr] = True
                        keep = True

            # keep it according to schedule, or keep it because it is in the keep_objects list
            if keep or thisobject in keep_objects or thisobject in always_keep_objects:
                keeps.append(thisobject)
            else:
                removes.append(thisobject)

        return keeps, removes