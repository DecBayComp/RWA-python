
from __future__ import absolute_import

from .generic import *


try:
    from scipy.sparse import bsr_matrix, coo_matrix, csc_matrix, csr_matrix, \
        dia_matrix, dok_matrix, lil_matrix
except ImportError:
    sparse_storables = []
else:
    # scipy.sparse storable instances mostly for Python2
    bsr_exposes = ['shape', 'data', 'indices', 'indptr']
    def mk_bsr(shape, data, indices, indptr):
        return bsr_matrix((data, indices, indptr), shape=shape)
    bsr_handler = handler(mk_bsr, bsr_exposes)

    coo_exposes = ['shape', 'data', 'row', 'col']
    def mk_coo(shape, data, row, col):
        return coo_matrix((data, (row, col)), shape=shape)
    coo_handler = handler(mk_coo, coo_exposes)

    csc_exposes = ['shape', 'data', 'indices', 'indptr']
    def mk_csc(shape, data, indices, indptr):
        return csc_matrix((data, indices, indptr), shape=shape)
    csc_handler = handler(mk_csc, csc_exposes)

    csr_exposes = ['shape', 'data', 'indices', 'indptr']
    def mk_csr(shape, data, indices, indptr):
        return csr_matrix((data, indices, indptr), shape=shape)
    csr_handler = handler(mk_csr, csr_exposes)

    dia_exposes = ['shape', 'data', 'offsets']
    def mk_dia(shape, data, offsets):
        return dia_matrix((data, offsets), shape=shape)
    dia_handler = handler(mk_dia, dia_exposes)

    # previously
    def dok_recommend(*args, **kwargs):
        raise TypeErrorWithAlternative('dok_matrix', 'coo_matrix')
    dok_handler = StorableHandler(poke=dok_recommend, peek=dok_recommend)
    # now
    def dok_poke(service, matname, mat, *args, **kwargs):
        coo_handler.poke(service, matname, mat.tocoo(), *args, **kwargs)
    def dok_peek(*args, **kwargs):
        return coo_handler.peek(*args, **kwargs).todok()
    dok_handler = StorableHandler(poke=dok_poke, peek=dok_peek)

    # previously
    def lil_recommend(*args, **kwargs):
        raise TypeErrorWithAlternative('lil_matrix', ('csr_matrix', 'csc_matrix'))
    lil_handler = StorableHandler(poke=lil_recommend, peek=lil_recommend)
    # now
    def lil_poke(service, matname, mat, *args, **kwargs):
        csr_handler.poke(service, matname, mat.tocsr(), *args, **kwargs)
    def lil_peek(*args, **kwargs):
        return csr_handler.peek(*args, **kwargs).tolil()
    lil_handler = StorableHandler(poke=lil_poke, peek=lil_peek)


    sparse_storables = [Storable(bsr_matrix, handlers=bsr_handler), \
        Storable(coo_matrix, handlers=coo_handler), \
        Storable(csc_matrix, handlers=csc_handler), \
        Storable(csr_matrix, handlers=csr_handler), \
        Storable(dia_matrix, handlers=dia_handler), \
        Storable(dok_matrix, handlers=dok_handler), \
        Storable(lil_matrix, handlers=lil_handler)]


spatial_storables = []
try:
    import scipy.spatial
except ImportError:
    pass
else:
    # scipy.sparse storable instances for Python2.
    # Python3 can autoserialize ConvexHull and may actually do a better job
    Delaunay_exposes = ['points', 'simplices', 'neighbors', 'equations', 'paraboloid_scale', 'paraboloid_shift', 'transform', 'vertex_to_simplex', 'convex_hull', 'coplanar', 'vertex_neighbor_vertices']
    ConvexHull_exposes = ['points', 'vertices', 'simplices', 'neighbors', 'equations', 'coplanar', 'area', 'volume']
    Voronoi_exposes = ['points', 'vertices', 'ridge_points', 'ridge_vertices', 'regions', 'point_region']

    _scipy_spatial_types = [
        ('Delaunay', Delaunay_exposes, ('simplices', )),
        ('ConvexHull', ConvexHull_exposes, ('vertices', 'equations')),
        ('Voronoi', Voronoi_exposes, ('regions', 'point_region'))]

    def scipy_spatial_storable(name, exposes, check):
        _fallback = namedtuple(name, exposes)
        _type = getattr(scipy.spatial.qhull, name)
        def _init(*args):
            struct = _type(args[0])
            check_attrs = list(check) # copy
            ok = True
            while ok and check_attrs:
                attr = check_attrs.pop()
                i = exposes.index(attr)
                try:
                    arg = getattr(struct, attr)
                    if isinstance(args[i], list):
                        ok = arg == args[i]
                    else:
                        ok = numpy.all(numpy.isclose(arg, args[i]))
                except (SystemExit, KeyboardInterrupt):
                    raise
                except:
                    #raise # debug
                    ok = False
            if not ok:
                warn('object of type {} cannot be properly regenerated; using method-less fallback'.format(name), RuntimeWarning)
                struct = _fallback(*args)
            return struct
        handlers = [handler(_init, exposes, version=(0,))] # Py2
        if six.PY3:
            auto = default_storable(_type)
            assert not auto.handlers[1:]
            assert handlers[0].version[0] < auto.handlers[0].version[0]
            handlers.append(auto.handlers[0])
        return Storable(_type, handlers=handlers)

    spatial_storables += \
        [ scipy_spatial_storable(*_specs) for _specs in _scipy_spatial_types ]

