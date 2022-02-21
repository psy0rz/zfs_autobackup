import hashlib

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
import time


def block_hash(fname, count=10000, bs=4096):
    """This function was created to checksum huge files and blockdevices (TB's)
    Instead of one sha1sum of the whole file, it generates sha1susms of chunks of the file.

    yields sha1 hash of fname,  per count blocks.
    yields(chunk_nr, hexdigest)

    yields nothing for empty files.

    """

    with open(fname, "rb") as f:
        hash = hashlib.sha1()
        block_nr = 0
        chunk_nr = 0
        for block in iter(lambda: f.read(bs), b""):
            hash.update(block)
            block_nr = block_nr + 1
            if block_nr % count == 0:
                yield (chunk_nr, hash.hexdigest())
                chunk_nr = chunk_nr + 1
                hash = hashlib.sha1()

        # yield last (incomplete) block
        if block_nr % count != 0:
            yield (chunk_nr, hash.hexdigest())

def block_hash_tree(start_path, count=10000, bs=4096):
    """block_hash every file in a tree, yielding the results

    note that it only checks the contents of actual files. It ignores metadata like permissions and mtimes.
    It also ignores empty directories, symlinks and special files.
    """

    cwd=os.getcwd()
    os.chdir(start_path)

    def walkerror(e):
        raise e

    try:
        for (dirpath, dirnames, filenames) in os.walk(".", onerror=walkerror):
            for f in filenames:
                file_path=os.path.join(dirpath, f)[2:]

                if (not os.path.islink(file_path)) and os.path.isfile(file_path):
                    for (chunk_nr, hash) in block_hash(file_path, count, bs):
                        yield ( file_path, chunk_nr, hash )
    finally:
        os.chdir(cwd)


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

