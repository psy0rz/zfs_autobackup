from .ZfsAuto import ZfsAuto


class ZfsAutoverify(ZfsAuto):
    """The zfs-autoverify class"""

    def __init__(self, argv, print_arguments=True):

        # NOTE: common options and parameters are in ZfsAuto
        super(ZfsAutoverify, self).__init__(argv, print_arguments)

    def parse_args(self, argv):
        """do extra checks on common args"""

        args=super(ZfsAutoverify, self).parse_args(argv)


        return args

    def get_parser(self):
        """extend common parser with  extra stuff needed for zfs-autobackup"""

        parser=super(ZfsAutoverify, self).get_parser()


        return (parser)

    def run(self):

        pass

def cli():
    import sys

    sys.exit(ZfsAutoverify(sys.argv[1:], False).run())


if __name__ == "__main__":
    cli()
