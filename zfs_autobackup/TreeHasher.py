import itertools
import os


class TreeHasher():
    """uses BlockHasher recursively on a directory tree

    Input and output generators are in the format: ( relative-filepath, chunk_nr, hexdigest)

    """

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

        def walkerror(e):
            raise e

        for (dirpath, dirnames, filenames) in os.walk(start_path, onerror=walkerror):
            for f in filenames:
                file_path=os.path.join(dirpath, f)

                if (not os.path.islink(file_path)) and os.path.isfile(file_path):
                    for (chunk_nr, hash) in self.block_hasher.generate(file_path):
                        yield ( os.path.relpath(file_path,start_path), chunk_nr, hash )


    def compare(self, start_path, generator):
        """reads from generator and compares blocks

        yields mismatches in the form: ( relative_filename, chunk_nr, compare_hexdigest, actual_hexdigest )
        yields errors in the form:     ( relative_filename, chunk_nr, compare_hexdigest, "message" )

        """

        count=0

        def filter_file_name( file_name, chunk_nr, hexdigest):
                return ( chunk_nr, hexdigest )


        for file_name, group_generator in itertools.groupby(generator, lambda x: x[0]):
            count=count+1
            block_generator=itertools.starmap(filter_file_name, group_generator)
            for ( chunk_nr, compare_hexdigest, actual_hexdigest) in self.block_hasher.compare(os.path.join(start_path,file_name), block_generator):
                yield ( file_name, chunk_nr, compare_hexdigest, actual_hexdigest )




