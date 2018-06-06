
"""
Test serialization of custom class objects and Python-3 autoserialization feature for HDF5.
"""

from rwa.generic import *
from rwa.hdf5 import HDF5Store, hdf5_storable

import os.path
import six
from collections import OrderedDict


class TestSerialization(object):

        def test_slots(self, tmpdir):
                test_file = os.path.join(tmpdir.strpath, 'test.h5')
                # test type
                class Class1(object):
                        __slots__ = ('attr1', 'attr2')
                        def __init__(self, attr1=None, attr2=None):
                                self.attr1 = attr1
                                self.attr2 = attr2
                        def __eq__(self, other):
                                return self.attr1 == other.attr1 and self.attr2 == other.attr2
                if six.PY2:
                        # Python2 does not make it automatically
                        hdf5_storable(default_storable(Class1))
                # test values
                data = {'slots1': Class1(.2, 'ab')}
                # write
                store = HDF5Store(test_file, 'w')
                try:
                        for t in data:
                                store.poke(t, data[t])
                finally:
                        store.close()
                # read and check
                store = HDF5Store(test_file, 'r')
                try:
                        for t in store.store:
                                obj = store.peek(t)
                                assert type(obj) is type(data[t])
                                assert obj == data[t]
                finally:
                        store.close()


        def test_map(self, tmpdir):
                test_file = os.path.join(tmpdir.strpath, 'test.h5')
                # test type
                class Class2(object):
                        def __init__(self, items={}):
                                self.items = items
                        def __eq__(self, other):
                                return self.items == other.items
                        def __nonzero__(self):
                                return bool(self.items)
                        def __len__(self):
                                return len(self.items)
                        def __iter__(self):
                                return iter(self.items)
                        def __getitem__(self, key):
                                return self.items[key]
                        def __setitem__(self, key, val):
                                self.items[key] = val
                if six.PY2:
                        # Python2 does not make it automatically
                        hdf5_storable(default_storable(Class2, exposes=('items',)))
                # test values
                data = {'map2': Class2(dict(((1, 2.), (0, 'ab'))))}
                # write
                store = HDF5Store(test_file, 'w')
                try:
                        for t in data:
                                store.poke(t, data[t])
                finally:
                        store.close()
                # read and check
                store = HDF5Store(test_file, 'r')
                try:
                        for t in store.store:
                                obj = store.peek(t)
                                assert type(obj) is type(data[t])
                                assert obj == data[t]
                finally:
                        store.close()

