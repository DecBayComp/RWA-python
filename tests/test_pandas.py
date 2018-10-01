# -*- coding: utf-8 -*-

"""
Test serialization to HDF5 of pandas types.
"""

from rwa.generic import *
from rwa.hdf5 import HDF5Store

import os.path
import numpy as np
from pandas import Index, Int64Index, Float64Index, MultiIndex, Series, DataFrame
try:
    from pandas import UInt64Index
except ImportError:
    _test_uint64index = False
else:
    _test_uint64index = True
try:
    from pandas import RangeIndex
except ImportError:
    _test_rangeindex = False
else:
    _test_rangeindex = True
try:
    from pandas import CategoricalIndex
except ImportError:
    _test_categoricalindex = False
else:
    _test_categoricalindex = True


def as_unicode(s):
    if isinstance(s, strtypes):
        if isinstance(s, bytes):
            s = s.decode('utf-8')
    elif s is not None:
        s = type(s)([ _s.decode('utf-8') if isinstance(_s, bytes) else _s for _s in s ])
    return s


class TestPandasTypes(object):

    def test_num_indices(self, tmpdir):
        test_file = os.path.join(tmpdir.strpath, 'test.h5')
        # test values
        array = np.r_[np.arange(1,4), 5, np.arange(7,10)]
        data = {'index0': Index(array, name=b'i0'),
            'index1': Index(list(b'abde')),
            'int64index': Int64Index(array.astype(np.int64), name=b'i64'),
            'float64index': Float64Index(array.astype(np.float64))}
        if _test_uint64index:
            data['uint64index'] = UInt64Index(array.astype(np.uint64))
        #
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
                print(t)
                val = store.peek(t)
                assert type(val) is type(data[t])
                assert val.dtype == data[t].dtype
                assert np.all(val.values == data[t].values)
                assert val.name == as_unicode(data[t].name)
        finally:
            store.close()

    def test_rangeindex(self, tmpdir):
        if not _test_rangeindex:
            assert False # pandas.RangeIndex not available
            return
        test_file = os.path.join(tmpdir.strpath, 'test.h5')
        # test value
        t = 'rangeindex'
        data = RangeIndex(start=10, stop=1, step=-2, name=b'a')
        # write
        store = HDF5Store(test_file, 'w')
        try:
            store.poke(t, data)
        finally:
            store.close()
        # read and check
        store = HDF5Store(test_file, 'r')
        try:
            val = store.peek(t)
            assert type(val) is type(data)
            assert val._start == data._start
            assert val._stop == data._stop
            assert val._step == data._step
            assert val.name == as_unicode(data.name)
        finally:
            store.close()

    def test_categoricalindex(self, tmpdir):
        if not _test_categoricalindex:
            assert False # pandas.CategoricalIndex not available
            return
        assert False # not implemented yet
        test_file = os.path.join(tmpdir.strpath, 'test.h5')
        # test value
        data = {'categoricalindex0': CategoricalIndex(list('aabacbba'), ordered=True),
            'categoricalindex1': CategoricalIndex([2, 1, 1, 3, 2, 3, 1],
                categories=[1, 0, 2, 3], name=b'ci1')}
        #
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
                print(t)
                val = store.peek(t)
                assert type(val) is type(data[t])
                assert np.all(val.codes == data[t].codes)
                assert val.categories.tolist() == as_unicode(data[t].categories.tolist())
                assert val.ordered == data[t].ordered
                assert val.name == as_unicode(data[t].name)
        finally:
            store.close()

    def test_multiindex(self, tmpdir):
        test_file = os.path.join(tmpdir.strpath, 'test.h5')
        # test value
        t = 'multiindex'
        data = MultiIndex.from_product([[1,2], [b'red',b'blue']], names=(b'number',b'color'))
        # write
        store = HDF5Store(test_file, 'w')
        try:
            store.poke(t, data)
        finally:
            store.close()
        # read and check
        store = HDF5Store(test_file, 'r')
        try:
            val = store.peek(t)
            assert type(val) is type(data)
            # get rid of pandas.core.base.FrozenList
            assert list( list(i) for i in val.levels ) == list( list(i) for i in data.levels )
            assert list( list(i) for i in val.labels ) == list( as_unicode(list(i)) for i in data.labels )
            assert list(val.names) == as_unicode(list(data.names))
        finally:
            store.close()

    def test_series(self, tmpdir):
        test_file = os.path.join(tmpdir.strpath, 'test.h5')
        # test values
        data = {'s1': Series(np.random.rand(10)),
            's2': Series(np.random.rand(10), index=[.1, .2, .3, .4, .5, .7, .8, .9, .92, .94]),
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
                print(t)
                s = store.peek(t)
                assert type(s) is type(data[t])
                assert s.dtype == data[t].dtype
                assert s.size == data[t].size
                assert np.all(s.values == data[t].values)
        finally:
            store.close()

    def test_dataframe(self, tmpdir):
        test_file = os.path.join(tmpdir.strpath, 'test.h5')
        # test values
        data = {'df1': DataFrame({u'a': np.arange(2,6), b'b': np.random.rand(4)}),
            'df2': DataFrame({u'a': np.arange(2,6), b'b': np.random.rand(4)},
                index=MultiIndex.from_product([[1,2], [b'red',b'blue']], names=(b'number',b'color'))),
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
                print(t)
                df = store.peek(t)
                assert type(df) is type(data[t])
                #assert df.index == data[t].index
                assert df.columns.tolist() == as_unicode(df.columns.tolist())
                assert np.all( t1 == t2 for t1,t2 in zip(df.dtypes, data[t].dtypes))
                assert df.shape == data[t].shape
                assert np.all(df.values == data[t].values)
        finally:
            store.close()


