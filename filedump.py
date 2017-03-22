import os
import pickle


class FileDump:

    def __init__(self, path, n_per_file=None, meta_file="metadata"):
        ''' Stores a lot of stuff in a lot of files.
            n_per_file: number of elements per file
                        needs to be set, when creating a new FileDump
                        needs to be None, when loading an old FileDump
            meta_file: name of the metadata file '''

        # path to the data directory
        self._path = path

        # path to the meta data file
        self._meta = os.path.join(path, meta_file)

        # create data directory if not exists
        if not os.path.isdir(self._path):
            os.makedirs(self._path)

        # check if meta file exists
        # if yes: load meta data
        # if no:  init meta data
        if os.path.isfile(self._meta):
            if n_per_file is not None:
                raise Exception("metafile found, cannot set n_per_file to {0}"
                                .format(n_per_file))
            self._load_metadata()
        else:
            if n_per_file is None:
                raise Exception("no metafile found,"
                                "cannot set n_per_file to None")
            self._n_per_file, self._n_stored = (n_per_file, 0)

        # create the handle to write
        # the meta data and commit
        # the current meta data state
        self._meta_file_handle = open(self._meta, "wb")
        self._update_metadata()

        # initialize the pointers
        # to the current data file
        # and update them to point
        # to the correct one
        self._current_data_file = 0
        self._data_file_handle = None
        self._select_datafile()

    def _load_metadata(self):
        ''' load the meta data file '''

        with open(self._meta, "rb") as f:
            self._n_per_file, self._n_stored = pickle.load(f)

    def _update_metadata(self):
        ''' commits the current meta data to the meta data file '''

        self._meta_file_handle.seek(0)
        meta = (self._n_per_file, self._n_stored)
        pickle.dump(meta, self._meta_file_handle)

    def _select_datafile(self):
        ''' selects the current data file which we want to use
            to commit our data '''
        # compute current data file index
        curr_data_file = int(self._n_stored / self._n_per_file)
        # check if file handle exits
        # or if we need to open a new one
        if self._data_file_handle is None\
           or self._current_data_file != curr_data_file:

            # close old handle (if exists)
            if self._data_file_handle is not None:
                self._data_file_handle.close()

            # open the current data file
            self._current_data_file = curr_data_file
            data_path = os.path.join(self._path, str(curr_data_file))
            self._data_file_handle = open(data_path, "ab")

    def dump(self, data):
        ''' dumps a data object into the store '''

        self._select_datafile()
        pickle.dump(data, self._data_file_handle)
        self._n_stored += 1
        self._update_metadata()

    def read(self, begin, end):
        ''' returns an iterator on the data objects
            stored at indices begin <= i < end '''

        # ensure that all data file handles are closed
        if self._data_file_handle is not None:
            self._data_file_handle.close()

        # bound checks
        if not end > begin or begin < 0 or end < 0:
            raise Exception("invalid bounds {0} {1}".format(begin, end))

        if end > self._n_stored:
            raise Exception("out of bounds {0} > {1}"
                            .format(end, self._n_stored))

        # compute first and last index
        # of data files containing our range
        begin_file = int(begin / self._n_per_file)
        end_file = int(end / self._n_per_file) + 1

        # start at the first position of the first file
        pos = begin_file * self._n_per_file

        # iterate over all data files
        for curr_file in range(begin_file, end_file):
            data_path = os.path.join(self._path, str(curr_file))
            with open(data_path, "rb") as f:

                # compute the size of the chunk to be returned
                # for this file
                chunk = min(pos + self._n_per_file, end)

                # iterate over all stored elements and yield
                # those that are in our range
                for i in range(pos, chunk):
                    r = pickle.load(f)
                    if i >= begin:
                        yield r

                pos += self._n_per_file

    def close(self):
        ''' closes are open file handles '''
        self._meta_file_handle.close()
        self._data_file_handle.close()

    def __len__(self):
        return self._n_stored

    def __iter__(self):
        if self._n_stored > 0:
            for i in self.read(0, self._n_stored):
                yield i

    def __enter__(self):
        return self

    def __exit__(self, a, b, c):
        self.close()
