# root@psyt14s:/home/psy/zfs_autobackup# ls -lh /home/psy/Downloads/carimage.zip
# -rw-rw-r-- 1 psy psy 990M Nov 26  2020 /home/psy/Downloads/carimage.zip
# root@psyt14s:/home/psy/zfs_autobackup# time sha1sum /home/psy/Downloads/carimage.zip
# a682e1a36e16fe0d0c2f011104f4a99004f19105  /home/psy/Downloads/carimage.zip
#
# real	0m2.558s
# user	0m2.105s
# sys	0m0.448s
# root@psyt14s:/home/psy/zfs_autobackup# time python3 -m zfs_autobackup.ZfsCheck
#
# real	0m1.459s
# user	0m0.993s
# sys	0m0.462s

# NOTE: surprisingly sha1 in via python3 is faster than the native sha1sum utility, even in the way we use below!
import os
import platform
import sys


def tmp_name(suffix=""):
    """create temporary name unique to this process and node"""

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
