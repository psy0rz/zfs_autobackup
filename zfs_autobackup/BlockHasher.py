import hashlib
import os


class BlockHasher():
    """This class was created to checksum huge files and blockdevices (TB's)
    Instead of one sha1sum of the whole file, it generates sha1susms of chunks of the file.

    The chunksize is count*bs (bs is the read blocksize from disk)

    Its also possible to only read a certain percentage of blocks to just check a sample.

    Input and output generators are in the format ( chunk_nr, hexdigest )

    NOTE: skipping is only used on the generator side. The compare side just compares what it gets from the input generator.

    """

    def __init__(self, count=10000, bs=4096, hash_class=hashlib.sha1, skip=0):
        self.count = count
        self.bs = bs
        self.chunk_size=bs*count
        self.hash_class = hash_class

        # self.coverage=coverage
        self.skip=skip
        self._skip_count=0

        self.stats_total_bytes=0


    def _seek_next_chunk(self, fh, fsize):
        """seek fh to next chunk and update skip counter.
        returns chunk_nr
        return false it should skip the rest of the file


        """

        #ignore rempty files
        if fsize==0:
            return False

        # need to skip chunks?
        if self._skip_count > 0:
            chunks_left = ((fsize - fh.tell()) // self.chunk_size) + 1
            # not enough chunks left in this file?
            if self._skip_count >= chunks_left:
                # skip rest of this file
                self._skip_count = self._skip_count - chunks_left
                return False
            else:
                # seek to next chunk, reset skip count
                fh.seek(self.chunk_size * self._skip_count, os.SEEK_CUR)
                self._skip_count = self.skip
                return  fh.tell()//self.chunk_size
        else:
            # should read this chunk, reset skip count
            self._skip_count = self.skip
            return fh.tell() // self.chunk_size

    def generate(self, fname):
        """Generates checksums

        yields(chunk_nr, hexdigest)

        yields nothing for empty files.
        """


        with open(fname, "rb") as fh:

            fh.seek(0, os.SEEK_END)
            fsize=fh.tell()
            fh.seek(0)

            while fh.tell()<fsize:
                chunk_nr=self._seek_next_chunk(fh, fsize)
                if chunk_nr is False:
                    return

                #read chunk
                hash = self.hash_class()
                block_nr = 0
                while block_nr != self.count:
                    block=fh.read(self.bs)
                    if block==b"":
                        break
                    hash.update(block)
                    block_nr = block_nr + 1

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