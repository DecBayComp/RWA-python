
import six
from .storable import *
from collections import deque, OrderedDict
import copy
import warnings
import traceback
import importlib


strtypes = str
try: # Py2
        strtypes = (strtypes, unicode)
except NameError: # Py3
        strtypes = (strtypes, bytes)
basetypes = (bool, int, float) + strtypes

def isreference(a):
        check = ('__dict__', '__slots__')
        for attr in check:
                try:
                        getattr(a, attr)
                except:
                        pass
                else:
                        return True
        return False


def lookup_type(storable_type):
        if storable_type.startswith('Python'):
                _, module_name = storable_type.split('.', 1)
        else:
                module_name = storable_type
        #type_name, module_name = \
        names = [ _name[::-1] for _name in module_name[::-1].split('.', 1) ]
        if names[1:]:
                type_name, module_name = names
        else:
                type_name = names[0]
                return eval(type_name)
        try:
                module = importlib.import_module(module_name)
                python_type = getattr(module, type_name)
        except (ImportError, AttributeError):
                python_type = None
        return python_type


def _add_to_stack(stack, element):
        return stack if stack is None else stack + (element,)


class GenericStore(StoreBase):
        __slots__ = ('verbose', '_stack_active')

        def __init__(self, storables, verbose=False):
                StoreBase.__init__(self, storables)
                self.verbose = verbose

        def registerStorable(self, storable):
                if not storable.handlers:
                        storable = default_storable(storable.python_type, version=storable.version, \
                                exposes=storable.exposes, storable_type=storable.storable_type)
                StoreBase.registerStorable(self, storable)

        def strRecord(self, record, container):
                return record

        def formatRecordName(self, objname):
                """abstract method"""
                raise NotImplementedError('abstract method')

        def newContainer(self, objname, obj, container):
                """abstract method"""
                raise NotImplementedError('abstract method')

        def getRecord(self, objname, container):
                """abstract method"""
                raise NotImplementedError('abstract method')

        def getRecordAttr(self, attr, record):
                """abstract method"""
                raise NotImplementedError('abstract method')

        def setRecordAttr(self, attr, val, record):
                """abstract method"""
                raise NotImplementedError('abstract method')

        def isStorable(self, record):
                return self.getRecordAttr('type', record) is not None

        def isNativeType(self, obj):
                return True # per default, so that `tryPokeAny` is not called

        def pokeNative(self, objname, obj, container):
                """abstract method"""
                raise TypeError('record not supported')

        def pokeStorable(self, storable, objname, obj, container, visited=None, _stack=None):
                #print((objname, storable.storable_type)) # debug
                storable.poke(self, objname, obj, container, visited=visited, _stack=_stack)
                try:
                        record = self.getRecord(objname, container)
                except KeyError:
                        # fake storable; silently skip
                        if self.verbose:
                                print("skipping `{}` (type: {})".format(objname, storable.storable_type))
                                if 1 < self.verbose:
                                        print(traceback.format_exc())
                else:
                        self.setRecordAttr('type', storable.storable_type, record)
                        if storable.version is not None:
                                self.setRecordAttr('version', from_version(storable.version), record)

        def pokeVisited(self, objname, obj, record, existing, visited=None, _stack=None):
                if self.hasPythonType(obj):
                        storable = self.byPythonType(obj).asVersion()
                        self.pokeStorable(storable, objname, obj, record, visited=visited, \
                                _stack=_stack)
                else:
                        try:
                                self.pokeNative(objname, obj, record)
                        except (SystemExit, KeyboardInterrupt):
                                raise
                        except:
                                self.dump_stack(_stack)
                                raise

        def poke(self, objname, obj, record, visited=None, _stack=None):
                top_call = _stack is None
                if top_call:
                        _stack = CallStack()
                try:
                        if visited is None:
                                # `visited` is supposed to be a singleton
                                # and should be initialized at the top `poke` call,
                                # before it is passed to other namespaces
                                visited = dict()
                        if objname == '__dict__':
                                ptr = _stack.pointer
                                # expand the content of the `__dict__` dictionary
                                __dict__ = obj
                                for objname, obj in __dict__.items():
                                        self.poke(objname, obj, record, visited=visited, _stack=_stack)
                                        # rewind the stack up to __dict__'s parent
                                        _stack.pointer = ptr
                        elif obj is not None:
                                ptr = _stack.add(objname)
                                if self.verbose:
                                        if self.hasPythonType(obj):
                                                typetype = 'storable'
                                        else:   typetype = 'native'
                                        print('writing `{}` ({} type: {})'.format(objname, \
                                                typetype, type(obj).__name__))
                                objname = self.formatRecordName(objname)
                                if isreference(obj):
                                        try:
                                                previous = visited[id(obj)]
                                        except KeyError:
                                                pass
                                        else:
                                                return self.pokeVisited(objname, obj, record, previous, \
                                                        visited=visited, _stack=_stack)
                                        visited[id(obj)] = (record, objname)
                                if self.hasPythonType(obj):
                                        storable = self.byPythonType(obj).asVersion()
                                        self.pokeStorable(storable, objname, obj, record, visited=visited, \
                                                _stack=_stack)
                                elif self.isNativeType(obj):
                                        self.pokeNative(objname, obj, record)
                                else:
                                        self.tryPokeAny(objname, obj, record, visited=visited, \
                                                _stack=_stack)
                                # rewind the stack
                                _stack.pointer = ptr
                except (SystemExit, KeyboardInterrupt):
                        raise
                except Exception as e:
                        if top_call:
                                raise _stack.exception(e)
                        else:
                                raise

        def tryPokeAny(self, objname, obj, record, visited=None, _stack=None):
                """abstract method"""
                raise NotImplementedError('abstract method')

        def peekNative(self, record):
                """abstract method"""
                raise TypeError('record not supported')

        def peekStorable(self, storable, record, _stack=None):
                return storable.peek(self, record, _stack=_stack)

        def peek(self, objname, container, _stack=None):
                top_call = _stack is None
                if top_call:
                        _stack = CallStack()
                try:
                        ptr = _stack.add(objname)
                        record = self.getRecord(self.formatRecordName(objname), container)
                        if self.isStorable(record):
                                t = self.getRecordAttr('type', record)
                                v = self.getRecordAttr('version', record)
                                try:
                                        #print((objname, self.byStorableType(t).storable_type)) # debugging
                                        storable = self.byStorableType(t).asVersion(v)
                                except KeyError:
                                        storable = self.defaultStorable(storable_type=t, version=to_version(v))
                                obj = self.peekStorable(storable, record, _stack=_stack)
                        else:
                                #print(objname) # debugging
                                obj = self.peekNative(record)
                        _stack.pointer = ptr
                        return obj
                except (SystemExit, KeyboardInterrupt):
                        raise
                except Exception as e:
                        if top_call:
                                raise _stack.exception(e)
                        else:
                                raise

        def defaultStorable(self, python_type=None, storable_type=None, version=None, **kwargs):
                if python_type is None:
                        python_type = lookup_type(storable_type)
                if self.verbose:
                        print('generating storable instance for type: {}'.format(python_type))
                self.storables.registerStorable(default_storable(python_type, \
                                version=version, storable_type=storable_type), **kwargs)
                return self.byPythonType(python_type, True).asVersion(version)


# pokes
def poke(exposes):
        def _poke(store, objname, obj, container, visited=None, _stack=None):
                try:
                        sub_container = store.newContainer(objname, obj, container)
                except:
                        raise ValueError('generic poke not supported by store')
                #_stack = _add_to_stack(_stack, objname)
                for iobjname in exposes:
                        try:
                                iobj = getattr(obj, iobjname)
                        except AttributeError:
                                pass
                        else:
                                store.poke(iobjname, iobj, sub_container, visited=visited, \
                                        _stack=_stack)
        return _poke

def poke_assoc(store, objname, assoc, container, visited=None, _stack=None):
        try:
                sub_container = store.newContainer(objname, assoc, container)
        except:
                raise ValueError('generic poke not supported by store')
        escape_keys = assoc and not all(isinstance(iobjname, strtypes) for iobjname,_ in assoc)
        reported_item_counter = 0
        escaped_key_counter = 0
        try:
                if escape_keys:
                        store.setRecordAttr('key', 'escaped', sub_container)
                        verbose = store.verbose # save state
                        for obj in assoc:
                                store.poke(str(escaped_key_counter), obj, sub_container, \
                                        visited=visited, _stack=_stack)
                                escaped_key_counter += 1
                                if store.verbose:
                                        reported_item_counter += 1
                                        if reported_item_counter == 9:
                                                store.verbose = False
                                                print('...')
                        store.verbose = verbose # restore state
                else:
                        for iobjname, iobj in assoc:
                                store.poke(iobjname, iobj, sub_container, visited=visited, \
                                        _stack=_stack)
        except TypeError as e:
                msg = 'wrong type for keys in associative list'
                if e.args[0].startswith(msg):
                        raise
                else:
                        raise TypeError("{}:\n\t{}".format(msg, e.args[0]))


# peeks
def default_peek(python_type, exposes):
        with_args = False
        make = python_type
        try:
                make()
        except:
                make = lambda: python_type.__new__(python_type)
                try:
                        make()
                except:
                        make = lambda args: python_type.__new__(python_type, *args)
                        with_args = True
        def missing(attr):
                return AttributeError("can't set attribute '{}' ({})".format(attr, python_type))
        if with_args:
                def peek(store, container, _stack=None):
                        state = []
                        for attr in exposes: # force order instead of iterating over `container`
                                #print((attr, attr in container)) # debugging
                                if attr in container:
                                        state.append(store.peek(attr, container, _stack=_stack))
                                else:
                                        state.append(None)
                        return make(state)
        elif '__dict__' in exposes:
                def peek(store, container, _stack=None):
                        obj = make()
                        for attr in container:
                                val = store.peek(attr, container, _stack=_stack)
                                try:
                                        setattr(obj, attr, val)
                                except AttributeError:
                                        raise missing(attr)
                        return obj
        else:
                def peek(store, container, _stack=None):
                        obj = make()
                        for attr in exposes: # force order instead of iterating over `container`
                                #print((attr, attr in container)) # debugging
                                if attr in container:
                                        val = store.peek(attr, container, _stack=_stack)
                                else:
                                        val = None
                                try:
                                        setattr(obj, attr, val)
                                except AttributeError:
                                        raise missing(attr)
                        return obj
        return peek

def unsafe_peek(init):
        def peek(store, container, _stack=None):
                return init(*[ store.peek(attr, container, _stack=_stack) for attr in container ])
        return peek

def peek_with_kwargs(init, args=[]):
        def peek(store, container, _stack=None):
                return init(\
                        *[ store.peek(attr, container, _stack=_stack) for attr in args ], \
                        **dict([ (attr, store.peek(attr, container, _stack=_stack)) \
                                for attr in container if attr not in args ]))
        return peek

def peek(init, exposes, debug=False):
        def _peek(store, container, _stack=None):
                args = [ store.peek(objname, container, _stack=_stack) \
                        for objname in exposes ]
                if debug:
                        print(args)
                return init(*args)
        return _peek

def peek_assoc(store, container, _stack=None):
        assoc = []
        try:
                if store.getRecordAttr('key', container) == 'escaped':
                        for i in container:
                                assoc.append(store.peek(i, container, _stack=_stack))
                else:
                        for i in container:
                                assoc.append((store.strRecord(i, container), store.peek(i, container, _stack=_stack)))
                #print(assoc) # debugging
        except TypeError as e:
                try:
                        for i in container:
                                pass
                        raise e
                except TypeError:
                        raise TypeError("container is not iterable; peek is not compatible\n\t{}".format(e.args[0]))
        return assoc



## routines for the automatic generation of storable instances for classes

def most_exposes(python_type):
        """
        Core engine for the automatic generation of storable instances.

        Finds the attributes exposed by the objects of a given type.

        Mostly Python3-only.
        Does not handle types which `__new__` method requires extra arguments either.

        Arguments:

                python_type (type): object type.

        Returns:

                list: attributes exposed.

        """
        _exposes = set()
        try:
                # list all standard class attributes and methods:
                do_not_expose = set(python_type.__dir__(object) + \
                        ['__slots__', '__module__', '__weakref__']) # may raise `AttributeError`
                empty = python_type.__new__(python_type) # may raise `TypeError`
        except AttributeError: # Py2 does not have `__dir__`
                try:
                        _exposes = python_type.__slots__
                except AttributeError:
                        pass
        except TypeError: # `__new__` requires input arguments
                for _workaround in storable_workarounds:
                        try:
                                _exposes = _workaround(python_type)
                        except (KeyboardInterrupt, SystemExit):
                                raise
                        except:
                                pass
                        else:
                                break
        else:
                # note that slots from parent classes are not in `__dict__` (like all slots)
                # and - in principle - not in `__slots__` either.
                all_members = empty.__dir__() # all slots are supposed to appear in this list
                for attr in all_members:
                        if attr in do_not_expose:
                                # note that '__dict__' is in `do_not_expose` (comes from `object`)
                                continue
                        try: # identify the methods and properties
                                getattr(empty, attr)
                        except AttributeError as e: # then `attr` might be a slot
                                # properties can still throw an `AttributeError`;
                                # try to filter some more out
                                if e.args:
                                        msg = e.args[0]
                                        if msg == attr or msg.endswith("' object has no attribute '{}'".format(attr)):
                                                _exposes.add(attr)
                        except:
                                pass
                for attr in ('__dict__',):
                        if attr in all_members:
                                _exposes.add(attr)
        return list(_exposes)

def namedtuple_exposes(_type):
        return _type._fields


expose_extensions = []

expose_extensions.append(most_exposes)

expose_extensions.append(namedtuple_exposes)


def default_storable(python_type, exposes=None, version=None, storable_type=None, peek=default_peek):
        """
        Default mechanics for building the storable instance for a type.

        Arguments:

                python_type (type): type.

                exposes (list): list of attributes exposed by the type.

                version (tuple): version number.

                storable_type (str): universal string identifier for the type.

                peek (callable): peeking routine.

        Returns:

                Storable: storable instance.

        """
        if not exposes:
                for extension in expose_extensions:
                        try:
                                exposes = extension(python_type)
                        except (KeyboardInterrupt, SystemExit):
                                raise
                        except:
                                pass
                        else:
                                if exposes:
                                        break
                if not exposes:
                        raise AttributeError('`exposes` required for type: {!r}'.format(python_type))
        return Storable(python_type, key=storable_type, \
                handlers=StorableHandler(version=version, exposes=exposes, \
                poke=poke(exposes), peek=peek(python_type, exposes)))


def kwarg_storable(python_type, exposes=None, version=None, storable_type=None, init=None, args=[]):
        warnings.warn('kwarg_storable', DeprecationWarning)
        if init is None:
                init = python_type
        if exposes is None:
                try:
                        exposes = python_type.__slots__
                except:
                        # take __dict__ and sort out the class methods
                        raise AttributeError('either define the `exposes` argument or the `__slots__` attribute for type: {!r}'.format(python_type))
        return Storable(python_type, key=storable_type, handlers=StorableHandler(version=version, \
                poke=poke(exposes), peek=peek_with_kwargs(init, args), exposes=exposes))


# standard sequences
def seq_to_assoc(seq):
        return [ (six.b(str(i)), x) for i, x in enumerate(seq) ]
def assoc_to_list(assoc):
        return [ x for _, x in sorted(assoc, key=lambda a: int(a[0])) ]

def poke_seq(v, n, s, c, visited=None, _stack=None):
        poke_assoc(v, n, seq_to_assoc(s), c, visited=visited, _stack=_stack)
def poke_dict(v, n, d, c, visited=None, _stack=None):
        poke_assoc(v, n, d.items(), c, visited=visited, _stack=_stack)
def peek_list(s, c, _stack=None):
        return assoc_to_list(peek_assoc(s, c, _stack=_stack))
def peek_tuple(s, c, _stack=None):
        return tuple(peek_list(s, c, _stack=_stack))
def peek_set(s, c, _stack=None):
        return set(peek_list(s, c, _stack=_stack))
def peek_frozenset(s, c, _stack=None):
        return frozenset(peek_list(s, c, _stack=_stack))
def peek_dict(s, c, _stack=None):
        items = peek_assoc(s, c, _stack=_stack)
        #if items:
        #       if not all([ len(i) == 2 for i in items ]):
        #               print(items)
        #               raise ValueError('missing keys')
        #       if len(set(k for k,_ in items)) < len(items):
        #               print(items)
        #               raise ValueError('duplicate keys')
        return dict(items)
def peek_deque(s, c, _stack=None):
        return deque(peek_list(s, c, _stack=_stack))
def peek_OrderedDict(s, c, _stack=None):
        return OrderedDict(peek_assoc(s, c, _stack=_stack))

seq_storables = [Storable(tuple, handlers=StorableHandler(poke=poke_seq, peek=peek_tuple)), \
        Storable(list, handlers=StorableHandler(poke=poke_seq, peek=peek_list)), \
        Storable(frozenset, handlers=StorableHandler(poke=poke_seq, peek=peek_frozenset)), \
        Storable(set, handlers=StorableHandler(poke=poke_seq, peek=peek_set)), \
        Storable(dict, handlers=StorableHandler(poke=poke_dict, peek=peek_dict)), \
        Storable(deque, handlers=StorableHandler(poke=poke_seq, peek=peek_deque)), \
        Storable(OrderedDict, handlers=StorableHandler(poke=poke_dict, peek=peek_OrderedDict)), \
        default_storable(memoryview, ['obj'])]


# helper for tagging unserializable types
def fake_poke(*args, **kwargs):
        pass
def fail_peek(unsupported_type):
        helper = "cannot deserializable type '{}'\n".format(unsupported_type)
        def peek(*args, **kwargs):
                def f(*args, **kwargs):
                        raise TypeError(helper)
                return f
        return peek
def not_storable(_type):
        """
        Helper for tagging unserializable types.

        Arguments:

                _type (type): type to be ignored.

        Returns:

                Storable: storable instance that does not poke.

        """
        return Storable(_type, handlers=StorableHandler(poke=fake_poke, peek=fail_peek(_type)))


class _Class(object):
        __slots__ = ('member_descriptor',)
        @property
        def property(self):
                pass
        def instancemethod(self):
                pass

function_storables = [ not_storable(_type) for _type in frozenset(( \
                type, \
                type(len), \
                type(lambda a: a), \
                type(_Class.member_descriptor), \
                type(_Class.property), \
                type(_Class.instancemethod), \
                type(_Class.__init__), \
                type(_Class().__init__), \
        )) ]


def poke_native(getstate):
        def poke(service, objname, obj, container, visited=None, _stack=None):
                service.pokeNative(objname, getstate(obj), container)
        return poke

def peek_native(make):
        def peek(service, container, _stack=None):
                return make(service.peekNative(container))
        return peek


type_storable = Storable(type, handlers=StorableHandler(
                        peek=peek_native(lookup_type),
                        poke=poke_native(format_type)))

def with_type_support(storables):
        _storables = []
        inserted = False
        for _storable in storables:
                if _storable.python_type is type:
                        _storable = type_storable
                        inserted = True
                _storables.append(_storable)
        if not inserted:
                _storables.append(type_storable)
        return _storables


try:
        import numpy
except ImportError:
        numpy_storables = []
else:
        # numpy.dtype
        numpy_storables = [\
                Storable(numpy.dtype, handlers=StorableHandler(poke=poke_native(lambda t: t.str), \
                        peek=peek_native(numpy.dtype)))]


def handler(init, exposes):
        return StorableHandler(poke=poke(exposes), peek=peek(init, exposes))


try:
        from scipy.sparse import bsr_matrix, coo_matrix, csc_matrix, csr_matrix, \
                dia_matrix, dok_matrix, lil_matrix
except ImportError:
        sparse_storables = []
else:
        # scipy.sparse storable instances
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
        def dok_poke(service, matname, mat, container, visited=None, _stack=None):
                coo_handler.poke(service, matname, mat.tocoo(), container, visited=visited, \
                        _stack=_stack)
        def dok_peek(service, container, _stack=None):
                return coo_handler.peek(service, container, _stack=_stack).todok()
        dok_handler = StorableHandler(poke=dok_poke, peek=dok_peek)

        # previously
        def lil_recommend(*args, **kwargs):
                raise TypeErrorWithAlternative('lil_matrix', ('csr_matrix', 'csc_matrix'))
        lil_handler = StorableHandler(poke=lil_recommend, peek=lil_recommend)
        # now
        def lil_poke(service, matname, mat, container, visited=None, _stack=None):
                csr_handler.poke(service, matname, mat.tocsr(), container, visited=visited, \
                        _stack=_stack)
        def lil_peek(service, container, _stack=None):
                return csr_handler.peek(service, container, _stack=_stack).tolil()
        lil_handler = StorableHandler(poke=lil_poke, peek=lil_peek)


        sparse_storables = [Storable(bsr_matrix, handlers=bsr_handler), \
                Storable(coo_matrix, handlers=coo_handler), \
                Storable(csc_matrix, handlers=csc_handler), \
                Storable(csr_matrix, handlers=csr_handler), \
                Storable(dia_matrix, handlers=dia_handler), \
                Storable(dok_matrix, handlers=dok_handler), \
                Storable(lil_matrix, handlers=lil_handler)]


try:
        import scipy.spatial
except ImportError:
        spatial_storables = []
else:
        # scipy.sparse storable instances for Python2 (Python3 can autoserialize ConvexHull)
        ConvexHull_exposes = ['points', 'vertices', 'simplices', 'neighbors', 'equations', 'coplanar', 'area', 'volume']
        def mk_ConvexHull(points, vertices, simplices, neighbors, equations, coplanar, area, volume):
                hull = scipy.spatial.qhull.ConvexHull(points)
                try:
                        ok = np.all(numpy.isclose(hull.vertices, vertices))
                except:
                        ok = False
                if not ok:
                        warn('object of type ConvexHull has changed', RuntimeWarning)
                return hull
        ConvexHull_handler = handler(mk_ConvexHull, ConvexHull_exposes)

        spatial_storables = [Storable(scipy.spatial.qhull.ConvexHull, handlers=ConvexHull_handler)]


try:
        import pandas
except ImportError:
        pandas_storables = []
else:
        def poke_index(service, name, obj, container, visited=None, _stack=None):
                poke_seq(service, name, obj.tolist(), container, visited=visited, _stack=_stack)
        def peek_index(init=pandas.Index):
                def pandas_index_peek(service, container, _stack=None):
                        return init(peek_list(service, container, _stack=_stack))
                return pandas_index_peek
        peek_multiindex = peek_with_kwargs(pandas.MultiIndex)
        #poke_multiindex = poke(['levels', 'labels', 'names'])
        # convert all the pandas.core.base.FrozenList into tuples
        def poke_multiindex(service, ixname, ix, parent_container, visited=None, _stack=None):
                try:
                        container = service.newContainer(ixname, ix, parent_container)
                except:
                        raise ValueError('generic poke not supported by store')
                for attrname in ('levels', 'labels'):
                        attr = tuple( tuple(item) for item in getattr(ix, attrname) )
                        service.poke(attrname, attr, container, visited=visited, _stack=_stack)
                attrname = 'names'
                attr = tuple( getattr(ix, attrname) )
                service.poke(attrname, attr, container, visited=visited, _stack=_stack)

        # as such in latest pandas versions; force it in old (Python2?) pandas versions to be the same
        pandas_storables = [Storable(pandas.Index, \
                 key='Python.pandas.core.index.Index', \
                 handlers=StorableHandler(poke=poke_index, peek=peek_index())), \
                Storable(pandas.Int64Index, \
                 key='Python.pandas.core.index.Int64Index', \
                 handlers=StorableHandler(poke=poke_index, peek=peek_index(pandas.Int64Index)))]

        try:
                # UInt64Index is mentioned in the documentation but is missing here
                pandas_storables.append( \
                        Storable(pandas.UInt64Index, \
                         key='Python.pandas.core.index.UInt64Index', \
                         handlers=StorableHandler(poke=poke_index, peek=peek_index(pandas.UInt64Index))))
        except AttributeError:
                pass

        pandas_storables += [ \
                Storable(pandas.Float64Index, \
                 key='Python.pandas.core.index.Float64Index', \
                 handlers=StorableHandler(poke=poke_index, peek=peek_index(pandas.Float64Index))), \
                Storable(pandas.MultiIndex, \
                 key='Python.pandas.core.index.MultiIndex', \
                 handlers=StorableHandler(poke=poke_multiindex, peek=peek_multiindex))]

        # `values` is not necessarily the underlying data; may be a coerced representation instead
        poke_series = poke(['data', 'index'])
        peek_series = peek(pandas.Series, ['data', 'index'], debug=True)
        if six.PY2:
                def poke_series(service, sname, s, parent_container, visited=None, _stack=None):
                        try:
                                container = service.newContainer(sname, s, parent_container)
                        except:
                                raise ValueError('generic poke not supported by store')
                        service.poke('data', s.values, container, visited=visited, _stack=_stack)
                        service.poke('index', s.index, container, visited=visited, _stack=_stack)
        # `poke_dataframe` is similar to `poke` but converts part of the dataframe into
        # an ordered dictionnary of columns
        def poke_dataframe(service, dfname, df, parent_container, visited=None, _stack=None):
                try:
                        container = service.newContainer(dfname, df, parent_container)
                except:
                        raise ValueError('generic poke not supported by store')
                data = OrderedDict([ (colname, df[colname].values) for colname in df.columns ])
                service.poke('data', data, container, visited=visited, _stack=_stack)
                service.poke('index', df.index, container, visited=visited, _stack=_stack)
        peek_dataframe = peek(pandas.DataFrame, ['data', 'index'])
        pandas_storables += [Storable(pandas.Series, handlers=StorableHandler(poke=poke_series, peek=peek_series)),
                Storable(pandas.DataFrame, handlers=StorableHandler(poke=poke_dataframe, peek=peek_dataframe))]



def namedtuple_storable(namedtuple, *args, **kwargs):
        return default_storable(namedtuple, namedtuple._fields, *args, **kwargs)

