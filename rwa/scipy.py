
from __future__ import absolute_import

from .generic import *


class ScipyStorable(Storable):
    def __init__(self, python_type, key=None, handlers=[]):
        Storable.__init__(self, python_type, key, handlers)
        self.deprivatize = True

class ScipySpatialStorable(ScipyStorable):
    @property
    def default_version(self):
        if six.PY2:
            return min([ h.version for h in self.handlers ])

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
        if any(s < 0 for s in shape):
            warnings.warn("corrupted shape: {}".format(shape))
            warnings.warn("data corruption is known to happen on newly created files and a known fix consists in restarting the Python interpreter session")
            return None
        if indptr[0] != 0:
            warnings.warn("corrupted first pointer (should be 0): {}".format(indptr[0]))
            warnings.warn("data corruption is known to happen on newly created files and a known fix consists in restarting the Python interpreter session")
            return None
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


    sparse_storables = [ScipyStorable(bsr_matrix, handlers=bsr_handler), \
        ScipyStorable(coo_matrix, handlers=coo_handler), \
        ScipyStorable(csc_matrix, handlers=csc_handler), \
        ScipyStorable(csr_matrix, handlers=csr_handler), \
        ScipyStorable(dia_matrix, handlers=dia_handler), \
        ScipyStorable(dok_matrix, handlers=dok_handler), \
        ScipyStorable(lil_matrix, handlers=lil_handler)]


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

    Delaunay_v1_exposes = [ '_points', 'coplanar', 'equations', 'good', 'max_bound', 'min_bound', 'ndim', 'neighbors', 'npoints', 'nsimplex', 'paraboloid_scale', 'paraboloid_shift', 'simplices', 'vertices' ]
    ConvexHull_v1_exposes = [ '_points', '_vertices', 'area', 'coplanar', 'equations', 'max_bound', 'min_bound', 'ndim', 'neighbors', 'npoints', 'nsimplex', 'simplices', 'volume' ]
    Voronoi_v1_exposes = [ '_points', 'max_bound', 'min_bound', 'ndim', 'npoints', 'point_region', 'regions', 'ridge_points', 'ridge_vertices', 'vertices' ]

    _scipy_spatial_types = [
        ('Delaunay', Delaunay_exposes, Delaunay_v1_exposes, ('simplices', )),
        ('ConvexHull', ConvexHull_exposes, ConvexHull_v1_exposes, ('vertices', 'equations')),
        ('Voronoi', Voronoi_exposes, Voronoi_v1_exposes, ('regions', 'point_region'))]

    def scipy_spatial_storable(name, exposes, v1_exposes, check):
        _fallback = namedtuple(name, exposes)
        _type = getattr(scipy.spatial.qhull, name)
        def _init(_exposes):
            def __init(*args):
                #print(args) # debug
                struct = _type(args[0])
                check_attrs = list(check) # copy
                ok = True
                while ok and check_attrs:
                    attr = check_attrs.pop()
                    try:
                        i = _exposes.index(attr)
                    except ValueError:
                        if attr[0] == '_':
                            attr = attr[1:]
                        else:
                            attr = '_'+attr
                    i = _exposes.index(attr)
                    try:
                        arg = getattr(struct, attr)
                        if isinstance(args[i], list):
                            ok = arg == args[i]
                        else:
                            ok = numpy.all(numpy.isclose(arg, args[i]))
                    except (SystemExit, KeyboardInterrupt):
                        raise
                    except:
                        #print(attr, arg, args[i]) # debug
                        raise # debug
                        #ok = False
                if not ok:
                    warn("object of type '{}' could not be properly regenerated from the `points` argument only; using method-free fallback".format(name), RuntimeWarning)
                    struct = _fallback(*args)
                return struct
            return __init
        handlers = [handler(_init(exposes), exposes, version=(0,))] # Py2
        if six.PY3:
            auto = default_storable(_type)
            assert not auto.handlers[1:]
            assert handlers[0].version[0] < auto.handlers[0].version[0]
            handlers.append(auto.handlers[0])
        elif six.PY2 and v1_exposes:
            handlers.append(handler(_init(v1_exposes), v1_exposes, version=(1,)))
        return ScipySpatialStorable(_type, handlers=handlers)

    spatial_storables += \
        [ scipy_spatial_storable(*_specs) for _specs in _scipy_spatial_types ]

