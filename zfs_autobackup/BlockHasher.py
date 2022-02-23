import hashlib


class BlockHasher():
    """This class was created to checksum huge files and blockdevices (TB's)
    Instead of one sha1sum of the whole file, it generates sha1susms of chunks of the file.

    The chunksize is count*bs (bs is the read blocksize from disk)

    Its also possible to only read a certain percentage of blocks to just check a sample.

    Input and output generators are in the format ( chunk_nr, hexdigest )
    """

    def __init__(self, count=10000, bs=4096, hash_class=hashlib.sha1, coverage=1):
        self.count = count
        self.bs = bs
        self.hash_class = hash_class
        self.coverage=1

        self.stats_total=0
        self.stats_checked=0

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

    def compare(self, fname, generator):
        """reads from generator and compares blocks
        Yields mismatches in the form: ( chunk_nr, hexdigest, actual_hexdigest)
        Yields errors in the form: ( chunk_nr, hexdigest, "message" )

        """

        try:
            checked = 0
            with open(fname, "rb") as f:
                for (chunk_nr, hexdigest) in generator:
                    try:

                        checked = checked + 1
                        hash = self.hash_class()
                        f.seek(int(chunk_nr) * self.bs * self.count)
                        block_nr = 0
                        for block in iter(lambda: f.read(self.bs), b""):
                            hash.update(block)
                            block_nr = block_nr + 1
                            if block_nr == self.count:
                                break

                        if block_nr == 0:
                            yield (chunk_nr, hexdigest, 'EOF')

                        elif (hash.hexdigest() != hexdigest):
                            yield (chunk_nr, hexdigest, hash.hexdigest())

                    except Exception as e:
                        yield ( chunk_nr , hexdigest, 'ERROR: '+str(e))

        except Exception as e:
            yield ( '-', '-', 'ERROR: '+ str(e))