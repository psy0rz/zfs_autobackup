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

import pathlib as pathlib


def block_hash(fname, count=10000, bs=4006):
    """yields sha1 hash per count blocks.
    yields(chunk_nr, hexdigest)

    yields nothing for empty files.

    This function was created to checksum huge files and blockdevices (TB's)
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
    """block_hash every file in a tree, yielding results"""

    os.chdir(start_path)

    for f in pathlib.Path('.').glob('**/*'):
        if f.is_file() and not f.is_symlink():
            for (chunk_nr, hash) in block_hash(f, count, bs):

                yield ( f, chunk_nr, hash)


def tmp_name(suffix=""):
    """create temporary name unique to this process and node"""

    #we could use uuids but those are ugly and confusing
    name="{}_{}_{}".format(
        os.path.basename(sys.argv[0]),
        platform.node(),
        os.getpid())
    name=name+suffix
    return name


def get_tmp_clone_name(snapshot):
    pool=snapshot.zfs_node.get_pool(snapshot)
    return pool.name+"/"+tmp_name()


#NOTE: https://www.google.com/search?q=Mount+Path+Limit+freebsd
#Freebsd has limitations regarding path length, so we have to clone it so the part stays sort
def activate_volume_snapshot(snapshot):
    """clone volume, waits and tries to findout /dev path to the volume, in a compatible way. (linux/freebsd/smartos)"""

    clone_name= get_tmp_clone_name(snapshot)
    clone=snapshot.clone(clone_name)

    #NOTE: add smartos location to this list as well
    locations=[
        "/dev/zvol/" + clone_name
    ]

    clone.debug("Waiting for /dev entry to appear...")
    time.sleep(0.1)

    start_time=time.time()
    while time.time()-start_time<10:
        for location in locations:
            stdout, stderr, exit_code=clone.zfs_node.run(["test", "-e", location], return_all=True, valid_exitcodes=[0,1])

            #fake it in testmode
            if clone.zfs_node.readonly:
                return location

            if exit_code==0:
                return location
        time.sleep(1)

    raise(Exception("Timeout while waiting for {} entry to appear.".format(locations)))


