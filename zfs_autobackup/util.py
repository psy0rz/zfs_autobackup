
# NOTE: surprisingly sha1 in via python3 is faster than the native sha1sum utility, even in the way we use below!
import os
import platform
import sys
from datetime import datetime


def tmp_name(suffix=""):
    """create temporary name unique to this process and node. always retruns the same result during the same execution"""

    #we could use uuids but those are ugly and confusing
    name="{}-{}-{}".format(
        os.path.basename(sys.argv[0]).replace(" ","_"),
        platform.node(),
        os.getpid())
    name=name+suffix
    return name


def get_tmp_clone_name(snapshot):
    pool=snapshot.zfs_node.get_pool(snapshot)
    return pool.name+"/"+tmp_name()



def output_redir():
    """use this after a BrokenPipeError to prevent further exceptions.
    Redirects stdout/err to /dev/null
    """

    devnull = os.open(os.devnull, os.O_WRONLY)
    os.dup2(devnull, sys.stdout.fileno())
    os.dup2(devnull, sys.stderr.fileno())

def sigpipe_handler(sig, stack):
    #redir output so we dont get more SIGPIPES during cleanup. (which my try to write to stdout)
    output_redir()
    #deb('redir')

# def check_output():
#     """make sure stdout still functions. if its broken, this will trigger a SIGPIPE which will be handled by the sigpipe_handler."""
#     try:
#         print(" ")
#         sys.stdout.flush()
#     except Exception as e:
#         pass

# def deb(txt):
#     with open('/tmp/debug.log', 'a') as fh:
#         fh.write("DEB: "+txt+"\n")


# This should be the only source of trueth for the current datetime.
# This function will be mocked during unit testing.


datetime_now_mock=None
def datetime_now(utc):
    if datetime_now_mock is None:
        return( datetime.utcnow() if utc else datetime.now())
    else:
        return datetime_now_mock
