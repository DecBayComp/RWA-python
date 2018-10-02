
from warnings import warn

class ConflictingVersionWarning(Warning):
    pass

_undefined_parent_error = RuntimeError('corrupted handlers in Storable')

class StorableHandler(object):
    '''Defines how to store an object of the class identified by `_parent`.

    Attributes:

        version (tuple): version number.

        exposes (iterable): list of attributes to get/set.

        _peek (callable): read object from store.
            To be called through :meth:`peek`.

        _poke (callable): write object in store.
            To be called through :meth:`poke`.

        _parent (Storable): storable instance this handler is associated to.

        _peek_option (set): keys of service-wide parameters to be passed to :attr:`_peek`.
            To be accessed through property :attr:`peek_option`.

        _poke_option (set): keys of service-wide parameters to be passed to :attr:`_poke`.
            To be accessed through property :attr:`poke_option`.

    :attr:`peek_option` and :attr:`poke_option` are keys in the service's :attr:`params` parameters
    which values are passed by :meth:`peek` and :meth:`poke` to :attr:`_peek` and :attr:`_poke`
    respectively, as keyword arguments if not already defined.

    A parameter key is supposed to be a dot-separated sequence of keywords,
    e.g. ``'my_module.my_option'``.
    The keyword passed to :func:`peek` or :func:`poke` will be the substring after the last dot.
    If :attr:`peek_option` is ``['my_module.my_option']``,
    then :func:`peek` will receive the ``my_option=params['my_module.my_option']`` argument
    (where *params* is the service-wide :attr:`~StorableService.params` parameters)
    if *my_option* is not already passed from the caller's context
    and '*my_module.my_option*' is defined in *params*.
    '''
    __slots__ = ('version', 'exposes', '_poke', '_peek', '_parent', \
            '_peek_option', '_poke_option')

    @property
    def peek_option(self):
        return self._peek_option
    @peek_option.setter
    def peek_option(self, keys):
        if keys is None:
            self._peek_option = set()
        elif isinstance(keys, (tuple, list, frozenset, set)):
            self._peek_option = set(keys)
        else:
            self._peek_option = set([keys])

    @property
    def poke_option(self):
        return self._poke_option
    @poke_option.setter
    def poke_option(self, keys):
        if keys is None:
            self._poke_option = set()
        elif isinstance(keys, (tuple, list, frozenset, set)):
            self._poke_option = set(keys)
        else:
            self._poke_option = set([keys])

    @property
    def python_type(self):
        if self._parent is None:
            raise _undefined_parent_error
        else:   return self._parent.python_type

    @property
    def storable_type(self):
        if self._parent is None:
            raise _undefined_parent_error
        else:   return self._parent.storable_type

    def __init__(self, version=None, exposes={}, peek=None, poke=None, peek_option=None, \
            poke_option=None):
        if version is None:
            version=(1,)
        self.version = version
        self.exposes = exposes
        self._parent = None
        self._peek = peek
        self._poke = poke
        self.peek_option = peek_option
        self.poke_option = poke_option

    def peek(self, *args, **kwargs):
        for option in self.peek_option:
            try:
                prm = self._parent.params[option]
            except KeyError:
                pass
            except AttributeError:
                raise _undefined_parent_error
            else:
                option = option.split('.')[-1]
                if option not in kwargs:
                    kwargs[option] = prm
        return self._peek(*args, **kwargs)

    def poke(self, *args, **kwargs):
        for option in self.poke_option:
            try:
                prm = self._parent.params[option]
            except KeyError:
                pass
            except AttributeError:
                raise _undefined_parent_error
            else:
                option = option.split('.')[-1]
                if option not in kwargs:
                    kwargs[option] = prm
        self._poke(*args, **kwargs)



class Storable(object):
    '''Describes a storable class.

    Attributes:

        python_type (type): type of the object to serialize.

        storable_type (str): unique identification string.

        _handlers (list): list of handlers.

        _parent (StorableService): service which the storable instance is registered in.

    Note that different copies of a storable instance should be registered into distinct
    services.
    '''
    __slots__ = ('python_type', 'storable_type', '_handlers', '_parent')

    def __init__(self, python_type, key=None, handlers=[]):
        self.python_type = python_type
        self.storable_type = key
        self._handlers = [] # PY2?
        self.handlers = handlers

    @property
    def handlers(self):
        return self._handlers

    @handlers.setter
    def handlers(self, handlers): # in PY2, setters work only in new style classes
        if isinstance(handlers, StorableHandler):
               handlers = [handlers]
        self._handlers = []
        for h in handlers:
            h = copy_handler(h)
            h._parent = self
            self._handlers.append(h)

    @property
    def params(self):
        return self._parent.params

    def hasVersion(self, version):
        return version in [ h.version for h in self.handlers ]

    def asVersion(self, version=None):
        if version is None:
            version = self.default_version
        else:
            version = to_version(version)
        handler = None
        if version is None:
            for h in self.handlers:
                if handler is None or handler.version < h.version:
                    handler = h
        else:
            for h in self.handlers:
                if h.version == version:
                    handler = h
                    break
            if handler is None:
                raise KeyError('no such version number: {}'.format(version))
        return handler

    @property
    def default_version(self):
        '''
        Default version (tuple) or ``None``. Read-only property to be overloaded.

        Example implementation:

        .. code-block:: python

            class MyStorableHandler(StorableHandler):
                @property
                def default_version(self):
                    if self.params.get('my_module.my_boolean_option', None):
                        return (2,) # default version is number 2
                    #else: return None

        '''
        return

    def poke(self, *args, **kwargs):
        self.asVersion(kwargs.pop('version', None)).poke(*args, **kwargs)

    def peek(self, *args, **kwargs):
        return self.asVersion(kwargs.pop('version', None)).peek(*args, **kwargs)


def format_type(python_type, agnostic=False):
    module = python_type.__module__
    name = python_type.__name__
    if module in ['__builtin__', 'builtins']:
        storable_type = name
    elif module.endswith(name):
        storable_type = module
    else:
        storable_type = module + '.' + name
    if not agnostic:
        storable_type = 'Python.' + storable_type
    return storable_type


class StorableService(object):
    '''Service for storable instances.

    Attributes:

        by_python_type (dict): dictionnary of storable instances with types as keys.

        by_storable_type (dict): dictionnary of storable instances with identification
            strings as keys.

        params (dict): mutable map of global parameters shared with all the registered
            storable instances.

    '''
    __slots__ = ('by_python_type', 'by_storable_type', 'params') # what about native_type?

    def __init__(self, params={}):
        self.by_python_type = {}
        self.by_storable_type = {}
        self.params = params

    def registerStorable(self, storable, replace=False, agnostic=False):
        # check for compliance and fill in missing fields if possible
        if not all([ isinstance(h.version, tuple) for h in storable.handlers ]):
            raise TypeError("`Storable`'s version should be a tuple of numerical scalars")
        if storable.storable_type is None:
            storable.storable_type = format_type(storable.python_type, agnostic)
        if not storable.handlers:
            raise ValueError('missing handlers', storable.storable_type)
        pokes = not all( h.poke is None for h in storable.handlers ) # not peek-only
        # get the existing storable with its handlers or make a storable with a single handler..
        if self.hasStorableType(storable.storable_type):
            existing = self.by_storable_type[storable.storable_type]
            if storable.python_type is not existing.python_type:
                raise TypeError('conflicting instances', storable.storable_type)
        else:
            if pokes and self.hasPythonType(storable.python_type, True):
                raise TypeError('conflicting instances', storable.python_type)
            existing = copy_storable(storable)
            existing._parent = self
            existing._handlers = []
        # .. and add the other/new handlers
        for h in storable.handlers:
            h._parent = existing
            if existing.hasVersion(h.version):
                if replace:
                    existing._handlers = [ h if h.version is h0.version else h0 \
                        for h0 in existing.handlers ]
                else:
                    warn(str((storable.storable_type, h.version)), ConflictingVersionWarning)
            else:
                existing._handlers.append(h)
        # place/replace the storable in the double dictionary
        if pokes:
            self.by_python_type[storable.python_type] = existing
        self.by_storable_type[storable.storable_type] = existing

    def byPythonType(self, t, istype=False):
        if istype:#isinstance(t, type):
            try:
                return self.by_python_type[t]
            except KeyError:
                return None
        else:
            #raise TypeError
            try:
                return self.by_python_type[type(t)]
            except KeyError:
                try:
                    return self.by_python_type[t.__class__]
                except (AttributeError, KeyError):
                    return None

    def hasPythonType(self, t, istype=False):
        if istype:#isinstance(t, type):
            return t in self.by_python_type
        else:
            if type(t) in self.by_python_type:
                return True
            else:
                try:
                    return t.__class__ in self.by_python_type
                except AttributeError:
                    return False

    def byStorableType(self, t):
        return self.by_storable_type[t]

    def hasStorableType(self, t):
        return t in self.by_storable_type



class StoreBase(StorableService):
    '''Proxy class to `StorableService` that defines two abstract methods to be implemented
    for each concrete store.

    Attributes:

        storables (StorableService): wrapped storable service.

    '''
    __slots__ = ('storables',)

    def __init__(self, storables):
        self.storables = storables

    @property
    def by_python_type(self):
        return self.storables.by_python_type

    @property
    def by_storable_type(self):
        return self.storables.by_storable_type

    def byPythonType(self, t, istype=False):
        return self.storables.byPythonType(t, istype)

    def hasPythonType(self, t):
        return self.storables.hasPythonType(t)

    def byStorableType(self, t):
        return self.storables.byStorableType(t)

    def hasStorableType(self, t):
        return self.storables.hasStorableType(t)

    def registerStorable(self, storable, **kwargs):
        self.storables.registerStorable(storable, **kwargs)

    def peek(self, objname, container, _stack=None):
        '''Reads from a container.

        Arguments:

            objname (str): object name.

            container (any): address of the object in the store.

            _stack (CallStack): stack of parent object names.

        Returns:

            any: deserialized object.

        '''
        raise NotImplementedError('abstract method')

    def poke(self, objname, obj, container, visited=None, _stack=None):
        '''Writes in a container.

        Arguments:

            objname (str): object name.

            obj (any): object to serialize.

            container (any): address of the object in the store.

            visited (dict): already seriablized objects.

            _stack (CallStack): stack of parent object names.

        '''
        raise NotImplementedError('abstract method')



def to_version(v):
    if isinstance(v, tuple):
        return v
    elif isinstance(v, list):
        return tuple(v)
    elif isinstance(v, int):
        return (v, )
    else:
        return tuple([ int(i) for i in v.split('.') ])

def from_version(v):
    s = '{:d}'
    s = s + ''.join( [ '.' + s ] * (len(v) - 1) )
    return s.format(*v)



class TypeErrorWithAlternative(TypeError):
    def __init__(self, instead_of, use):
        self.failing_type = instead_of
        self.suggested_type = use

    def __repr__(self):
        return '{} instead of {} use {}'.format(self.__class__, self.failing_type, self.suggested_type)

    def __str__(self):
        instead_of = self.failing_type
        use = self.suggested_type
        if isinstance(use, str):
            part1 = "use `{}` instead of `{}`".format(use, instead_of)
        else:
            s = "instead of `{}`, use any of the following:\n"
            s = ''.join([s] + [ "\t{}\n" ] * len(use))
            part1 = s.format(instead_of, *use)
            use = use[0]
        part2 = "If you can modify the parent class, please consider adding:\n" +\
            "\t@property\n" +\
            "\tdef _my_{}(self):\n" +\
            "\t\treturn # convert `my_{}` to `{}`\n" +\
            "\t@_my_{}.setter\n" +\
            "\t\tmy_{} = # convert `_my_{}` to `{}`\n" +\
            "and replace `my_{}` by `_my_{}` to the corresponding ReferenceHandler `exposes` attribute."
        part2 = part2.format(use, instead_of, use, use, instead_of, use, instead_of, instead_of, use)
        return part1 + part2


class CallStack(object):
    """
    Stack of `peek` or `poke` calls.

    (De-)serialized object names are recorded so that the top :meth:`~StoreBase.peek` or
    :meth:`~StoreBase.poke` call can report the stack of object names till that of the object
    that raised an exception.

    Each generic :meth:`~StoreBase.peek` or :meth:`~StoreBase.poke` call should first get the
    pointer, call the :meth:`add` method (that actually returns the pointer) and then set the
    pointer back to its original value before each child :meth:`~StoreBase.peek` or
    :meth:`~StoreBase.poke` call.
    """
    __slots__ = ('stack',)
    def __init__(self):
        self.stack = []
    def add(self, record):
        ptr = self.pointer
        self.stack.append(record)
        return ptr
    @property
    def pointer(self):
        return len(self.stack)
    @pointer.setter
    def pointer(self, ptr):
        if not isinstance(ptr, int) or ptr < 0 or len(self.stack) < ptr:
            raise ValueError('wrong pointer')
        self.stack = self.stack[:ptr]
    def __repr__(self):
        return 'CallStack'+str(self.stack)
    def __str__(self):
        if not self.stack:
            return ''
        else:
            intro = 'In: '
            first_tab = ' ' * len(intro)
            prefix = '|- '
            tab = ' ' * len(prefix)
            first_line = intro + self.stack[0]
            next_lines = [ ''.join((first_tab, tab*i, prefix, record))
                    for i, record in enumerate(self.stack[1:]) ]
            return '\n'.join([first_line] + next_lines)
    def exception(self, exc):
        return type(exc)('\n'.join((exc.args[0], str(self))), *exc.args[1:])
    def clear(self):
        self.stack = []
    def __nonzero__(self):
        return bool(self.stack)
    def __len__(self):
        return len(self.stack)
    def __getitem__(self, i):
        return self.stack[i]
    def __setitem__(self, i, record):
        self.stack[i] = record
    def __delitem__(self, record):
        self.stack.delete(record)
    def __reversed__(self):
        return reversed(self.stack)
    def __contains__(self, record):
        return record in self.stack
    def __missing__(self, i):
        return self.stack.__missing__(i)
    def pop(self):
        return self.stack.pop()


def copy_handler(handler):
    return StorableHandler(handler.version, handler.exposes, handler._peek, handler._poke, \
            handler.peek_option, handler.poke_option)

def copy_storable(storable, constructor=None):
    if constructor is None:
        constructor = type(storable)
    return constructor(storable.python_type, storable.storable_type, storable.handlers)

