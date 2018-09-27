# -*- coding: utf-8 -*-

"""
Test serialization to HDF5 of scipy.sparse and scipy.spatial types.
"""

from rwa.generic import *
from rwa.hdf5 import HDF5Store

import os.path
import numpy as np
from scipy import sparse, spatial


class TestSciPyTypes(object):

    def test_sparse_types(self, tmpdir):
        test_file = os.path.join(tmpdir.strpath, 'test.h5')
        # test values
        i = [0, 0, 1, 3, 3]
        j = [1, 3, 2, 0, 3]
        k = range(1, len(i)+1)
        shape = (4, 5)
        coo = sparse.coo_matrix((k, (i,j)), shape=shape)
        dense = coo.todense(True)
        data = {'coo': coo,
            'csr': sparse.csr_matrix((k, (i,j)), shape=shape),
            'csc': sparse.csc_matrix((k, (i,j)), shape=shape),
            'dia': sparse.dia_matrix(dense, copy=True),
            'dok': sparse.dok_matrix(dense, copy=True),
            'lil': sparse.lil_matrix(dense, copy=True),
            'bsr': sparse.bsr_matrix(dense, copy=True)}
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
                assert np.all(val.todense() == data[t].todense())
        finally:
            store.close()

    def test_spatial_types(self, tmpdir):
        test_file = os.path.join(tmpdir.strpath, 'test.h5')
        # test values
        data = {'convexhull': spatial.qhull.ConvexHull(np.random.rand(5,2)),
            'convexhull_with_options': spatial.qhull.ConvexHull(np.random.rand(5,2), qhull_options='QbB'),
            'delaunay': spatial.qhull.Delaunay(np.random.rand(5,2)),
            'voronoi': spatial.qhull.Voronoi(np.random.rand(5,2)),
            'voronoi_with_options': spatial.qhull.Voronoi(np.random.rand(5,2), True, qhull_options='Qbb Qz'),
               }
        #
        check = [('vertices', True),
            ('simplices', True),
            ('neighbors', True),
            ('equations', True),
            ('coplanar',  True),
            ('area',      False),
            ('volume',    False),
            ('paraboloid_scale', False),
            ('paraboloid_shift', False),
            #('transform', True),
            #('vertex_to_simplex', True),
            ('convex_hull', True),
            ('coplanar',  True),
            #('vertex_neighbor_vertices', False),
            ('ridge_points', True),
            ('ridge_vertices', False),
            ('regions',   False),
            ('point_region', True)]
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
                #assert type(val) is type(data[t])
                for attr, isarray in check:
                    try:
                        ref = getattr(data[t], attr)
                    except AttributeError:
                        pass
                    else:
                        test = getattr(val, attr)
                        if isarray:
                            assert np.all(np.isclose(test,  ref))
                        else:
                            assert test == ref
        finally:
            store.close()

