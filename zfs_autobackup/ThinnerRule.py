import re


class ThinnerRule:
    """a thinning schedule rule for Thinner"""

    TIME_NAMES = {
        'y': 3600 * 24 * 365.25,
        'm': 3600 * 24 * 30,
        'w': 3600 * 24 * 7,
        'd': 3600 * 24,
        'h': 3600,
        'min': 60,
        's': 1,
    }

    TIME_DESC = {
        'y': 'year',
        'm': 'month',
        'w': 'week',
        'd': 'day',
        'h': 'hour',
        'min': 'minute',
        's': 'second',
    }

    def __init__(self, rule_str):
        """parse scheduling string
            example:
                daily snapshot, remove after a week:     1d1w
                weekly snapshot, remove after a month:   1w1m
                monthly snapshot, remove after 6 months: 1m6m
                yearly snapshot, remove after 2 year:    1y2y
                keep all snapshots, remove after a day   1s1d
                keep nothing:                            1s1s

        """

        rule_str = rule_str.lower()
        matches = re.findall("([0-9]*)([a-z]*)([0-9]*)([a-z]*)", rule_str)[0]

        if '' in matches:
            raise (Exception("Invalid schedule string: '{}'".format(rule_str)))

        period_amount = int(matches[0])
        period_unit = matches[1]
        ttl_amount = int(matches[2])
        ttl_unit = matches[3]

        if period_unit not in self.TIME_NAMES:
            raise (Exception("Invalid period string in schedule: '{}'".format(rule_str)))

        if ttl_unit not in self.TIME_NAMES:
            raise (Exception("Invalid ttl string in schedule: '{}'".format(rule_str)))

        self.period = period_amount * self.TIME_NAMES[period_unit]
        self.ttl = ttl_amount * self.TIME_NAMES[ttl_unit]

        if self.period > self.ttl:
            raise (Exception("Period cant be longer than ttl in schedule: '{}'".format(rule_str)))

        self.rule_str = rule_str

        self.human_str = "Keep every {} {}{}, delete after {} {}{}.".format(
            period_amount, self.TIME_DESC[period_unit], period_amount != 1 and "s" or "", ttl_amount,
            self.TIME_DESC[ttl_unit], ttl_amount != 1 and "s" or "")

    def __str__(self):
        """get schedule as a schedule string"""

        return self.rule_str