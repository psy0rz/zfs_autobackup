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
def block_hash(fname, count=10000, bs=4006):
    """yields sha1 hash per count blocks.
    yields(chunk_nr, hexdigest)

    yields nothing for empty files.
    """

    with open(fname, "rb") as f:
        hash = hashlib.sha1()
        block_nr = 0
        chunk_nr = 0
        for block in iter(lambda: f.read(4096), b""):
            hash.update(block)
            block_nr = block_nr + 1
            if block_nr % count == 0:
                yield (chunk_nr, hash.hexdigest())
                chunk_nr = chunk_nr + 1
                hash = hashlib.sha1()

        # yield last (incomplete) block
        if block_nr % count != 0:
            yield (chunk_nr, hash.hexdigest())
