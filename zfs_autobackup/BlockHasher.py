import hashlib


class BlockHasher():
    """This class was created to checksum huge files and blockdevices (TB's)
    Instead of one sha1sum of the whole file, it generates sha1susms of chunks of the file.

    The chunksize is count*bs (bs is the read blocksize from disk)

    Its also possible to only read a certain percentage of blocks to just check a sample.
    """
    def __init__(self, count=10000, bs=4096, hash_class=hashlib.sha1):
        self.count=count
        self.bs=bs
        self.hash_class=hash_class


    def generate(self, fname):
        """Generates checksums

        yields(chunk_nr, hexdigest)

        yields nothing for empty files.
        """
        with open(fname, "rb") as f:
            hash = self.hash_class()
            block_nr = 0
            chunk_nr = 0
            for block in iter(lambda: f.read(self.bs), b""):
                hash.update(block)
                block_nr = block_nr + 1
                if block_nr % self.count == 0:
                    yield (chunk_nr, hash.hexdigest())
                    chunk_nr = chunk_nr + 1
                    hash = self.hash_class()

            # yield last (incomplete) block
            if block_nr % self.count != 0:
                yield (chunk_nr, hash.hexdigest())

        # def compare(fname, generator):
        #     """reads from generatos and compares blocks"""
        #
        #     with open(fname, "rb") as f:
        #         for ( count, bs , chunk_nr, hexdigest) in input_generator: