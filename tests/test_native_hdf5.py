# -*- coding: utf-8 -*-

"""
Test serialization to HDF5 of (h5py-)native types.
"""

from rwa.generic import *
from rwa.hdf5 import HDF5Store

import os.path
import numpy as np
from collections import OrderedDict


class TestNativeTypes(object):

        def test_base_types(self, tmpdir):
                # test types/values
                value = {'bool': True, 'int': 2, 'float': .1, 'complex': 2+3j,
                        'bytes': b'abc', 'unicode': u'abç'}
                value['str'] = value['bytes' if str is bytes else 'unicode']
                try:
                        value['long'] = long(9999999999999999)
                except NameError: # Py3
                        pass
                #
                k = str(bool).find('bool') # Py2 may yield 7 while Py3 may yield 8
                test_file = os.path.join(tmpdir.strpath, 'test.h5')
                # write
                store = HDF5Store(test_file, 'w')
                try:
                        for t in basetypes:
                                _type = str(t)[k:-2]
                                store.poke(_type, value[_type])
                finally:
                        store.close()
                # `long` is converted into `int` by h5py and this is actually desirable
                # for Py2-Py3 compatibility.
                if 'long' in value:
                        value['long'] = int(value['long'])
                # read and check
                store = HDF5Store(test_file, 'r')
                try:
                        for _type in store.store:
                                val = store.peek(_type)
                                # the assertion immediately below is redundant but helps debug when numpy hides its base types
                                assert not isinstance(val, (np.bool_, np.int_, np.float_))
                                assert type(val) is type(value[_type])
                finally:
                        store.close()

        def test_numpy_types(self, tmpdir):
                test_file = os.path.join(tmpdir.strpath, 'test.h5')
                # test values
                data = {'1darray': np.random.rand(5), '2darray': np.random.rand(2,2),
                        'structured': np.array(list(enumerate(np.random.rand(4))),
                                dtype=[('index', 'int32'), ('value', 'float32')]),
                        }
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
                                array = store.peek(t)
                                assert type(array) is type(data[t])
                                assert array.shape == data[t].shape
                                assert array.dtype == data[t].dtype
                finally:
                        store.close()

        def test_sequence(self, tmpdir):
                test_file = os.path.join(tmpdir.strpath, 'test.h5')
                # test values
                data = {'empty': [],
                        'tuple': (2, None, 'a'),
                        'list': [None, .1, 'b'],
                        'frozenset': frozenset((1, 1, 0, 2)),
                        'set': set(('j', 'a', 'j', 'k')),
                        }
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
                                seq = store.peek(t)
                                assert type(seq) is type(data[t])
                                assert seq == data[t]
                finally:
                        store.close()

        def test_map(self, tmpdir):
                test_file = os.path.join(tmpdir.strpath, 'test.h5')
                # test values
                seq = (2, 0, 3, 4, -1, -2, 1)
                data = {'standard': dict(zip(seq, seq)),
                        'ordered': OrderedDict(zip(seq, seq)),
                        'heterogenous items': dict(a=None, b=1, c='c', d=np.arange(4), e=set((2,3,4))),
                        'heterogenous keys': {1: 0, 'a': 1, .2: 2},
                        }
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
                                assoc = store.peek(t)
                                assert type(assoc) is type(data[t])
                                assert assoc == data[t] or \
                                        assoc == type(data[t])( (k, e) for k, e in assoc.items() if e is not None )
                finally:
                        store.close()

