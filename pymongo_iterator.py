"""
Author: Alex Cannan (alexcannan on Github)
Date Created: 4/13/20
Purpose: This file contains a PymongoIterator class that will automatically
reinitialize if the cursor is lost, for lengthy iterations that you'd rather
not have stop randomly.

Features:
- Automatic cursor reinitialization
- Works with tqdm
- Working search and sort options
- Working skip and limit parameters
- disp_progress parameter if you'd like, but I'd recommend just wrapping it
    in tqdm

TODO:
- Multiprocessing
- Feel free to PR here or open an issue to request more features
"""

from pymongo import MongoClient
from pymongo.errors import CursorNotFound


class PymongoIterator:
    """ Robust pymongo iterator that reinitializes if cursor is lost. """
    def __init__(self,
                 collection,
                 search_opts={},
                 sort_opts=[("$natural", 1)],
                 skip=0,
                 limit=0,
                 disp_progress=0):
        """ Initialize variables and cursor, finds total documents. """
        self.collection = collection
        self.search_opts = search_opts
        self.sort_opts = sort_opts
        self.skip = skip
        self.disp_progress = disp_progress
        if limit == 0:
            self.limit = 2**31
        else:
            self.limit = limit
        self.i = 0
        self.total_docs = self.collection.count_documents(search_opts,
                                                          skip=self.skip,
                                                          limit=self.limit)
        print("Iterating over", self.total_docs, "documents...")
        self.cursor = self.collection.find(search_opts,
                                           no_cursor_timeout=True) \
                                     .sort(self.sort_opts) \
                                     .skip(self.skip) \
                                     .limit(self.limit)

    def __iter__(self):
        """ Required for iterator """
        return self

    def __next__(self):
        """ If we haven't reached the end of the iterator yet, this tries to
        find the next document, and will reinitialize if needed. """
        if self.i >= self.total_docs:
            self.cursor.close()
            raise StopIteration
        try:
            self.doc = next(self.cursor)
            self.i += 1
            if self.disp_progress:
                if self.i % self.disp_progress == 0:
                    print("Parsing doc", self.i, "of", self.total_docs)
        except CursorNotFound:
            print("Lost cursor, reinitializing...")
            return self.__reinitialize()
        return self.doc

    def __len__(self):
        return self.total_docs

    def __reinitialize(self):
        """ If CursorNotFound raised, this reinitializes the cursor and returns
        the next document object. """
        if self.limit:
            intermediate_limit = self.limit - self.i
        else:
            intermediate_limit = 0
        self.cursor = self.collection.find(self.search_opts,
                                           no_cursor_timeout=True) \
                                     .skip(self.i+self.skip) \
                                     .limit(intermediate_limit)
        doc = next(self.cursor)
        self.i += 1
        return doc
