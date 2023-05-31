
import six
from .storable import *
from collections import namedtuple, deque, OrderedDict
import copy
import warnings
import traceback
import importlib


numtypes = (int, float, complex)
strtypes = str
try: # Py2
    strtypes = (strtypes, unicode)
    numtypes = (numtypes[0], long) + numtypes[1:]
except NameError: # Py3
    strtypes = (strtypes, bytes)
basetypes = (bool, ) + numtypes + strtypes


rwa_params = dict()


class ExplicitNone(object):
    """ *new in 0.8.5* """
    def __getattr__(self, attr):
        return None
    def __setattr__(self, attr, val):
        pass


def isreference(a):
    """
    Tell whether a variable is an object reference.

    Due to garbage collection, some objects happen to get the id of a distinct variable.
    As a consequence, linking is not ready yet and `isreference` returns ``False``.
    """
    return False
    return id(a) != id(copy.copy(a))
    check = ('__dict__', '__slots__')
    for attr in check:
        try:
            getattr(a, attr)
        except (SystemExit, KeyboardInterrupt):
            raise
        except:
            pass
        else:
            return True
    return False


def lookup_type(storable_type):
    """
    Look for the Python type that corresponds to a storable type name.
    """
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


class GenericStore(StoreBase):
    """
    Abstract class for stores.

    Implements the :meth:`poke` and :meth:`peek` methods with support for
    duplicate references and call stack (error reporting).
    Every other *poke*/*peek* functions should call these high-level methods to
    (de-)serialize children attributes.

    :meth:`poke` and :meth:`peek` delegate to specialized variants such as
    :meth:`pokeNative` and :meth:`pokeStorable` (or their *peek* counterparts).
    Children attribute names are also converted into record references in these
    high-level methods.

    """
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
        """
        Convert a record reference from a container into a string.

        The default implementation considers string record references that do not
        require convertion.

        See also :meth:`formatRecordName`.
        """
        return record

    def formatRecordName(self, objname):
        """
        Convert a record name into a record reference.

        **abstract method**

        The term *record reference* refers to the address of the record in the store
        as it can be natively understood by the underlying store.

        Arguments:

            objname (str): record/object name.

        Returns:

            any: record reference for use in all *peek*/*poke* methods except
                the main :meth:`peek` and :meth:`poke` methods.

        See also :meth:`strRecord`.
        """
        raise NotImplementedError('abstract method')

    def newContainer(self, objname, obj, container):
        """
        Make a new container.

        **abstract method**

        Arguments:

            objname (any): record reference.

            obj (any): object to be serialized into the record; useful only if
                the type of the record depends on the nature of the object.

            container (any): container.

        Returns:

            any: reference (address in the store) of the new container.
        """
        raise NotImplementedError('abstract method')

    def getRecord(self, objname, container):
        """
        Record getter.

        **abstract method**

        Arguments:

            objname (any): record reference.

            container (any): parent container.

        Returns:

            any: record or container.
        """
        raise NotImplementedError('abstract method')

    def getRecordAttr(self, attr, record):
        """
        Record attribute getter.

        **abstract method**

        Arguments:

            attr (str): attribute name.

            record (any): record.

        Returns:

            str: attribute string value.
        """
        raise NotImplementedError('abstract method')

    def setRecordAttr(self, attr, val, record):
        """
        Record attribute setter.

        **abstract method**

        Arguments:

            attr (str): attribute name.

            val (str): attribute string value.

            record (any): record.

        """
        raise NotImplementedError('abstract method')

    def isStorable(self, record):
        return self.getRecordAttr('type', record) is not None

    def isNativeType(self, obj):
        """
        Tell whether an object can be natively serialized.

        If ``True``, :meth:`poke` delegates to :meth:`pokeNative`.
        Otherwise, :meth:`poke` delegates to :meth:`tryPokeAny`.

        The default implementation returns ``True``.
        """
        return True # per default, so that `tryPokeAny` is not called

    def pokeNative(self, objname, obj, container):
        """
        Let the underlying store serialize an object.

        **abstract method**

        Arguments:

            objname (any): record reference.

            obj (any): object to be serialized.

            container (any): container or record.

        For stores with dict-like interface, :meth:`pokeNative` can be thought as:

        .. code-block:: python

            container[objname] = obj

        See also :meth:`peekNative`.
        """
        raise TypeError('record not supported')

    def pokeStorable(self, storable, objname, obj, container, visited=None, _stack=None, **kwargs):
        """
        Arguments:

            storable (StorableHandler): storable instance.

            objname (any): record reference.

            obj (any): object to be serialized.

            container (any): container.

            visited (dict): map of the previously serialized objects that are
                passed by references; keys are the objects' IDs.

            _stack (CallStack): stack of parent object names.

        Trailing keyword arguments are passed to the :class:`Storable` instance's
        :attr:`~Storable.poke`.
        """
        #print((objname, storable.storable_type)) # debug
        storable.poke(self, objname, obj, container, visited=visited, _stack=_stack, **kwargs)
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

    def pokeVisited(self, objname, obj, record, existing, visited=None, _stack=None, **kwargs):
        """
        Serialize an already serialized object.

        If the underlying store supports linking, this is the place where to make links.

        The default implementation delegates to :meth:`pokeStorable` or :meth:`pokeNative`.

        Arguments:

            objname (any): record reference.

            obj (any): object to be serialized.

            existing (any): absolute reference of the record which the object
                was already serialized into.

            visited (dict): already serialized objects.

            _stack (CallStack): stack of parent object names.

        """
        if self.hasPythonType(obj):
            storable = self.byPythonType(obj).asVersion()
            self.pokeStorable(storable, objname, obj, record, visited=visited, \
                _stack=_stack, **kwargs)
        else:
            try:
                self.pokeNative(objname, obj, record)
            except (SystemExit, KeyboardInterrupt):
                raise
            except:
                self.dump_stack(_stack)
                raise

    def poke(self, objname, obj, record, visited=None, _stack=None, **kwargs):
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
                    self.poke(objname, obj, record, visited=visited, _stack=_stack, **kwargs)
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
                            visited=visited, _stack=_stack, **kwargs)
                    visited[id(obj)] = (record, objname)
                if self.hasPythonType(obj):
                    storable = self.byPythonType(obj).asVersion()
                    self.pokeStorable(storable, objname, obj, record, visited=visited, \
                        _stack=_stack, **kwargs)
                elif self.isNativeType(obj):
                    self.pokeNative(objname, obj, record)
                else:
                    self.tryPokeAny(objname, obj, record, visited=visited, \
                        _stack=_stack, **kwargs)
                # rewind the stack
                _stack.pointer = ptr
        except (SystemExit, KeyboardInterrupt):
            raise
        except Exception as e:
            if top_call and not self.verbose:
                new_e = _stack.exception(e)
                new_e.__cause__ = None
                raise new_e
            else:
                raise

    def tryPokeAny(self, objname, obj, record, visited=None, _stack=None, **kwargs):
        """
        First try to poke with :meth:`pokeNative`.
        If this fails, generate a default storable instance and try with
        :meth:`pokeStorable` instead.

        **abstract method**

        See also :meth:`pokeStorable` for a description of the input arguments.

        See also :meth:`isNativeType` for how to route an object through :meth:`tryPokeAny`.
        """
        raise NotImplementedError('abstract method')

    def peekNative(self, record):
        """
        Let the underlying store deserialize an object.

        **abstract method**

        See also :meth:`pokeNative`.
        """
        raise TypeError('record not supported')

    def peekStorable(self, storable, record, _stack=None, **kwargs):
        """
        Arguments:

            storable (StorableHandler): storable instance.

            record (any): record.

            _stack (CallStack): stack of parent object names.

        Returns:

            any: deserialized object.

        Trailing keyword arguments are passed to the :class:`Storable` instance's
        :attr:`~Storable.peek`.
        """
        return storable.peek(self, record, _stack=_stack, **kwargs)

    def peek(self, objname, container, _stack=None, **kwargs):
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
                    try:
                        storable = self.defaultStorable(storable_type=t, version=to_version(v))
                    except AutoSerialFailure as e:
                        exc = self.diagnosePeekFailure(container, record, t, v, e)
                        if exc is None:
                            pass
                        else:
                            exc.__cause__ = None
                            raise exc
                try:
                    obj = self.peekStorable(storable, record, _stack=_stack, **kwargs)
                except (SystemExit, KeyboardInterrupt):
                    raise
                except Exception as e:
                    if self.verbose:
                        exc = self.diagnosePeekFailure(container, record, t, v, e)
                        if exc is None:
                            pass
                        else:
                            exc.__cause__ = None
                            raise exc
                    else:
                        raise
            else:
                #print(objname) # debugging
                obj = self.peekNative(record)
            _stack.pointer = ptr
            if isinstance(obj, ExplicitNone):
                return None
            else:
                return obj
        except (SystemExit, KeyboardInterrupt):
            raise
        except Exception as e:
            if top_call and not self.verbose:
                e_new = _stack.exception(e)
                e_new.__cause__ = None
                raise e_new
            else:
                raise

    def diagnosePeekFailure(self, container, record, _type, version, exception):
        tab = '  '
        try:
            msg = tab+exception.args[0].replace('\n', '\n'+tab)
        except IndexError: # no args
            return exception
        msg_fmt = ("Automatic peek" if isinstance(exception, AutoSerialFailure) else "Peek") + \
                " failed with error:\n{}\nAttributes found for type '{}' (v.{}):\n{}[ '{}' ]"
        msg = msg_fmt.format(msg, _type, version, tab, "', '".join(list(self.iterObjectNames(container))))
        exception.args = [msg] + list(exception.args[1:])
        return exception

    def defaultStorable(self, python_type=None, storable_type=None, version=None, **kwargs):
        """
        Generate a default storable instance.

        Arguments:

            python_type (type): Python type of the object.

            storable_type (str): storable type name.

            version (tuple): version number of the storable handler.

        Returns:

            StorableHandler: storable instance.

        Extra keyword arguments are passed to :meth:`registerStorable`.
        """
        if python_type is None:
            python_type = lookup_type(storable_type)
        if self.verbose:
            print('generating storable instance for type: {}'.format(python_type))
        self.storables.registerStorable(default_storable(python_type, \
                version=version, storable_type=storable_type), **kwargs)
        return self.byPythonType(python_type, True).asVersion(version)

    def iterObjectNames(self, container):
        return container


# pokes
def poke(exposes):
    """
    Default serializer factory.

    Arguments:

        exposes (iterable): attributes to serialized.

    Returns:

        callable: serializer (`poke` routine).
    """
    def _poke(store, objname, obj, container, visited=None, _stack=None):
        try:
            sub_container = store.newContainer(objname, obj, container)
        except (SystemExit, KeyboardInterrupt):
            raise
        #except:
        #    raise ValueError('generic poke not supported by store')# from None
        #_stack = _add_to_stack(_stack, objname)
        for iobjname in exposes:
            if isinstance(iobjname, (tuple, list)):
                iobjname, irecname = iobjname
            else:
                irecname = iobjname
            try:
                iobj = getattr(obj, iobjname)
            except AttributeError:
                pass
            else:
                store.poke(irecname, iobj, sub_container, visited=visited, \
                    _stack=_stack)
    return _poke

def poke_assoc(store, objname, assoc, container, visited=None, _stack=None):
    """
    Serialize association lists.
    """
    try:
        sub_container = store.newContainer(objname, assoc, container)
    except (SystemExit, KeyboardInterrupt):
        raise
    except:
        raise ValueError('generic poke not supported by store')# from None
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
            e_new = TypeError("{}:\n\t{}".format(msg, e.args[0]))
            raise e_new


# peeks
def default_peek(python_type, exposes, excess_attributes=[]):
    """
    Autoserializer factory.

    Works best in Python 3.

    Arguments:

        python_type (type): type constructor.

        exposes (iterable): sequence of attributes.

        excess_attributes (iterable): set of unrequired attributes that might
            have been serialized by other versions of the provider library.

    Returns:

        callable: deserializer (`peek` routine).

    """
    with_args = False
    make = python_type
    try:
        make()
    except (SystemExit, KeyboardInterrupt):
        raise
    except:
        make = lambda: python_type.__new__(python_type)
        try:
            make()
        except (SystemExit, KeyboardInterrupt):
            raise
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
                    if attr not in excess_attributes:
                        print(excess_attributes)
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
                    if attr not in excess_attributes:
                        raise missing(attr)
            return obj
    return peek

def unsafe_peek(init):
    """
    Deserialize all the attributes available in the container and pass them in the same order
    as they come in the container.

    This is a factory function; returns the actual `peek` routine.

    Arguments:

        init: type constructor.

    Returns:

        callable: deserializer (`peek` routine).

    """
    def peek(store, container, _stack=None):
        return init(*[ store.peek(attr, container, _stack=_stack) for attr in container ])
    return peek

def peek_with_kwargs(init, args=[], permissive=False):
    """
    Make datatypes passing keyworded arguments to the constructor.

    This is a factory function; returns the actual `peek` routine.

    Arguments:

        init (callable): type constructor.

        args (iterable): arguments NOT to be keyworded; order does matter.

        permissive (bool): missing positional arguments are set to None (*new in 0.8.5*).

    Returns:

        callable: deserializer (`peek` routine).

    All the peeked attributes that are not referenced in `args` are passed to `init` as
    keyworded arguments.
    """
    if permissive:
        def try_peek(store, attr, container, _stack=None):
            try:
                return store.peek(attr, container, _stack=_stack)
            except KeyError:
                return None
        def peek(store, container, _stack=None):
            return init(\
                *[ try_peek(store, attr, container, _stack) for attr in args ], \
                **dict([ (attr, store.peek(attr, container, _stack=_stack)) \
                    for attr in container if attr not in args ]))
    else:
        def peek(store, container, _stack=None):
            return init(\
                *[ store.peek(attr, container, _stack=_stack) for attr in args ], \
                **dict([ (attr, store.peek(attr, container, _stack=_stack)) \
                    for attr in container if attr not in args ]))
    return peek

peek_as_dict = peek_with_kwargs(dict)


def peek(init, exposes, debug=False):
    """
    Default deserializer factory.

    Arguments:

        init (callable): type constructor.

        exposes (iterable): attributes to be peeked and passed to `init`.

    Returns:

        callable: deserializer (`peek` routine).
    """
    def _peek(store, container, _stack=None):
        args = [ store.peek(objname, container, _stack=_stack) \
            for objname in exposes ]
        if debug:
            print(args)
        obj = init(*args)
        for i in store.iterObjectNames(container):
            objname = store.strRecord(i, container)
            if objname not in exposes:
                setattr(obj, objname, store.peek(objname, container, _stack=_stack))
        return obj
    return _peek


def peek_assoc(store, container, _stack=None):
    """
    Deserialize association lists.
    """
    assoc = []
    if store.getRecordAttr('key', container) == 'escaped':
        for i in store.iterObjectNames(container):
            assoc.append(store.peek(i, container, _stack=_stack))
    else:
        for i in store.iterObjectNames(container):
            assoc.append((store.strRecord(i, container), store.peek(i, container, _stack=_stack)))
    #print(assoc) # debugging
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
            except (SystemExit, KeyboardInterrupt):
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
            except (SystemExit, KeyboardInterrupt):
                raise
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


class AutoSerialFailure(NotImplementedError):
    def __init__(self, msg=None, typ=None):
        self.msg = msg
        self.typ = typ
    def __repr__(self):
        if self.typ:
            return ' '.join((self.msg, repr(self.typ)))
        else:
            return self.msg
    def __str__(self):
        return repr(self)


def default_storable(python_type, exposes=None, version=None, storable_type=None, peek=default_peek):
    """
    Default mechanics for building the storable instance for a type.

    Arguments:

        python_type (type): type.

        exposes (iterable): attributes exposed by the type.

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
            except (SystemExit, KeyboardInterrupt):
                raise
            except:
                pass
            else:
                if exposes:
                    break
        if not exposes:
            raise AutoSerialFailure('`exposes` required for type:', python_type)
    return Storable(python_type, key=storable_type, \
        handlers=StorableHandler(version=version, exposes=exposes, \
        poke=poke(exposes), peek=peek(python_type, exposes)))


def kwarg_storable(python_type, exposes=None, version=None, storable_type=None, init=None, args=[]):
    """
    **Deprecated**
    """
    warnings.warn('kwarg_storable', DeprecationWarning)
    if init is None:
        init = python_type
    if exposes is None:
        try:
            exposes = python_type.__slots__
        except (SystemExit, KeyboardInterrupt):
            raise
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


# range for Py <= 3.5
#if not isinstance(range, type):
#    def peek_range(s, c, _stack=None):
#        return range(
#                s.peek(c, '_start', _stack),
#                s.peek(c, '_stop', _stack),
#                s.peek(c, '_step', _stack))
#    class future_range(object):
#        pass
#    seq_storables.append(
#            Storable(future_range, 'Python.range', StorableHandler(poke=fake_poke, peek=peek_range)))


# helpers for services and already registered storable instances
def force_auto(service, _type):
    """
    Helper for forcing autoserialization of a datatype with already registered explicit
    storable instance.

    Arguments:

        service (StorableService): active storable service.

        _type (type): type to be autoserialized.

    **Not tested**
    """
    storable = service.byPythonType(_type, istype=True)
    version = max(handler.version[0] for handler in storable.handlers) + 1
    _storable = default_storable(_type, version=(version, ))
    storable.handlers.append(_storable.handlers[0])


# callables
class _Class(object):
    __slots__ = ('member_descriptor',)
    @property
    def property(self):
        pass
    def instancemethod(self):
        pass

function_storables = [ not_storable(_type) for _type in frozenset(( \
        #type, \
        type(len), \
        type(lambda a: a), \
        type(_Class.member_descriptor), \
        type(_Class.property), \
        type(_Class.instancemethod), \
        type(_Class.__init__), \
        type(_Class().__init__), \
    )) ]


def poke_native(getstate):
    """
    Serializer factory for types which state can be natively serialized.

    Arguments:

        getstate (callable): takes an object and returns the object's state
            to be passed to `pokeNative`.

    Returns:

        callable: serializer (`poke` routine).

    """
    def poke(service, objname, obj, container, visited=None, _stack=None):
        service.pokeNative(objname, getstate(obj), container)
    return poke

def peek_native(make):
    """
    Deserializer factory for types which state can be natively serialized.

    Arguments:

        make (callable): type constructor.

    Returns:

        callable: deserializer (`peek` routine)

    """
    def peek(service, container, _stack=None):
        return make(service.peekNative(container))
    return peek


type_storable = Storable(type, handlers=StorableHandler(
            peek=peek_native(lookup_type),
            poke=poke_native(format_type)))


def handler(init, exposes, version=None):
    """
    Simple handler with default `peek` and `poke` procedures.

    Arguments:

        init (callable): type constructor.

        exposes (iterable): attributes to be (de-)serialized.

        version (tuple): version number.

    Returns:

        StorableHandler: storable handler.
    """
    return StorableHandler(poke=poke(exposes), peek=peek(init, exposes), version=version)


def namedtuple_storable(namedtuple, *args, **kwargs):
    """
    Storable factory for named tuples.
    """
    return default_storable(namedtuple, namedtuple._fields, *args, **kwargs)


# NumPy
try:
    import numpy
except ImportError:
    numpy_storables = []
else:
    numpy_basic_types = (
        numpy.bool_, numpy.int_, numpy.intc, numpy.intp,
        numpy.int8, numpy.int16, numpy.int32, numpy.int64,
        numpy.uint8, numpy.uint16, numpy.uint32, numpy.uint64,
        numpy.float_, numpy.float16, numpy.float32, numpy.float64,
        numpy.complex_, numpy.complex64, numpy.complex128,
        )

    # numpy.dtype
    numpy_storables = [\
        Storable(numpy.dtype, handlers=StorableHandler(poke=poke_native(lambda t: t.str), \
            peek=peek_native(numpy.dtype)))]

