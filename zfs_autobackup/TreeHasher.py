import itertools
import os


class TreeHasher():
    """uses BlockHasher recursively on a directory tree"""

    def __init__(self, block_hasher):
        """

        :type block_hasher: BlockHasher
        """
        self.block_hasher=block_hasher

    def generate(self, start_path):
        """Use BlockHasher on every file in a tree, yielding the results

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
                        for (chunk_nr, hash) in self.block_hasher.generate(file_path):
                            yield ( file_path, chunk_nr, hash )
        finally:
            os.chdir(cwd)


    def compare(self, start_path, generator):
        """reads from generator and compares blocks, raises exception on error
        """

        cwd=os.getcwd()
        os.chdir(start_path)
        count=0
        try:

            def filter_file_name( file_name, chunk_nr, hexdigest):
                    return ( chunk_nr, hexdigest )


            for file_name, group_generator in itertools.groupby(generator, lambda x: x[0]):
                count=count+1
                block_generator=itertools.starmap(filter_file_name, group_generator)
                for ( chunk_nr, compare_hexdigest, actual_hexdigest) in self.block_hasher.compare(file_name, block_generator):
                    yield ( file_name, chunk_nr, compare_hexdigest, actual_hexdigest )
        finally:
            os.chdir(cwd)




