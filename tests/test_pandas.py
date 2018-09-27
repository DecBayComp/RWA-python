# -*- coding: utf-8 -*-

"""
Test serialization to HDF5 of pandas types.
"""

from rwa.generic import *
from rwa.hdf5 import HDF5Store

import os.path
import numpy as np
from pandas import Index, Int64Index, Float64Index, MultiIndex, Series, DataFrame


class TestPandasTypes(object):

    def test_ordered_indices(self, tmpdir):
        test_file = os.path.join(tmpdir.strpath, 'test.h5')
        # test values
        array = np.r_[np.arange(1,4), 5, np.arange(7,10)]
        data = {'index': Index(array),
            'int64index': Int64Index(array.astype(np.int64)),
            #'uint64index': UInt64Index(array.astype(np.uint64)),
            'float64index': Float64Index(array.astype(np.float64))}
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
                val = store.peek(t)
                assert type(val) is type(data[t])
                assert val.dtype == data[t].dtype
                assert np.all(val.values == data[t].values)
        finally:
            store.close()

    def test_multiindex(self, tmpdir):
        test_file = os.path.join(tmpdir.strpath, 'test.h5')
        # test value
        t = 'multiindex'
        data = MultiIndex.from_product([[1,2], [u'red',u'blue']], names=('number','color'))
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
            assert list( list(i) for i in val.labels ) == list( list(i) for i in data.labels )
            assert list(val.names) == list(data.names)
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
        data = {'df1': DataFrame({u'a': np.arange(2,6), u'b': np.random.rand(4)}),
            'df2': DataFrame({u'a': np.arange(2,6), u'b': np.random.rand(4)},
                index=MultiIndex.from_product([[1,2], [u'red',u'blue']], names=('number','color'))),
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
                df = store.peek(t)
                assert type(df) is type(data[t])
                #assert df.index == data[t].index
                assert np.all( t1 == t2 for t1,t2 in zip(df.dtypes, data[t].dtypes))
                assert df.shape == data[t].shape
                assert np.all(df.values == data[t].values)
        finally:
            store.close()


