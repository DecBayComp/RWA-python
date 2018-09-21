
from .generic import GenericStore
import rwa.generic as generic
from threading import RLock
import os
import shutil
import copy


class LazyStore(GenericStore):

        __slots__ = ('handle', '_lazy', '_default_lazy', '_lock', 'open_args', 'open_kwargs')

        def __init__(self, storables, verbose=False, **kwargs):
                GenericStore.__init__(self, storables, verbose)
                self.handle = None
                self._default_lazy = self._lazy = LazyPeek
                self._lock = RLock()
                self.open_args = ()
                self.open_kwargs = kwargs

        @property
        def lazy(self):
                return self._lazy is not None

        @lazy.setter
        def lazy(self, lazy):
                if _issubclass(lazy, LazyPeek):
                        self._lazy = lazy
                elif lazy:
                        self._lazy = self._default_lazy
                else:
                        self._lazy = None

        def open(self):
                if self.handle is None:
                        self.handle = self.__open__(*self.open_args, **self.open_kwargs)

        def __open__(self, *args, **kwargs):
                raise NotImplementedError('abstract method')

        def close(self):
                if self.handle is not None:
                        try:
                                self.__close__(self.handle)
                        finally:
                                self.handle = None

        def __close__(self, handle):
                handle.close()

        def peek(self, objname, container, lazy=None, **kwargs): # `_stack` has to be keyworded
                if lazy is None:
                        return GenericStore.peek(self, objname, container, **kwargs)
                else:
                        try:
                                past_value, self.lazy = self._lazy, lazy
                                return GenericStore.peek(self, objname, container, **kwargs)
                        finally:
                                self._lazy = past_value

        def peekStorable(self, storable, record, *args, **kwargs):
                if self.lazy:
                        return self._lazy(self, storable, record, *args, **kwargs)
                else:
                        return GenericStore.peekStorable(self, storable, record, *args, **kwargs)

        def poke(self, objname, obj, record, *args, **kwargs):
                GenericStore.poke(self, objname, lazyvalue(obj, deep=True), record, *args, **kwargs)

        def locator(self, record):
                return record

        def container(self, record_id):
                return record_id

        def lock(self, block=True):
                if self._lock.acquire(block):
                        self.open()
                        return True
                else:
                        return False

        def release(self):
                self._lock.release()


class LazyPeek(object):
        __slots__ = ('storable', 'store', 'locator', '_value', '_deep', '_stack')

        def __init__(self, store, storable, container, _stack=None):
                self.storable = storable
                self.store = store
                self.locator = store.locator(container)
                self._value = self._deep = None
                self._stack = copy.copy(_stack)

        def peek(self, deep=False, block=True):
                if self._value is None or (deep and not self._deep):
                        if not self.store.lock(block):
                                return
                        try:
                                previous, self.store.lazy = self.store.lazy, not deep
                                try:
                                        self._value = GenericStore.peekStorable(
                                                self.store,
                                                self.storable,
                                                self.store.container(self.locator),
                                                _stack=self._stack)
                                        self._deep = deep
                                except (SystemExit, KeyboardInterrupt):
                                        raise
                                except Exception as e:
                                        raise self._stack.exception(e)
                                finally:
                                        self.store.lazy = previous
                        finally:
                                self.store.release()
                return self._value

        def deep(self):
                return self.peek(True)

        def shallow(self):
                return self.peek()

        @property
        def value(self):
                return self.shallow()

        @property
        def type(self):
                return self.storable.python_type

        def permissive(self, true=True):
                if true:
                        new = PermissivePeek(self.store, self.storable, self.container,
                                _stack=self._stack)
                        new._value, new._deep = self._value, self._deep
                        return new
                else:
                        return self


class PermissivePeek(LazyPeek):

        def permissive(self, true=True):
                if true:
                        return self
                else:
                        new = LazyPeek(self.store, self.storable, self.container,
                                _stack=self._stack)
                        new._value, new._deep = self._value, self._deep
                        return new

        @property
        def value(self):
                return self.deep()

        def __getattr__(self, name):
                return getattr(self.value, name)

        def __nonzero__(self):
                return self.value.__nonzero__()

        def __len__(self):
                return self.value.__len__()

        def __getitem__(self, key):
                return self.value.__getitem__(key)

        def __missing__(self, key):
                return self.value.__missing__(key)

        def __iter__(self):
                return self.value.__iter__()

        def __reversed__(self):
                return self.value.__reversed__()

        def __contains__(self, item):
                return self.value.__contains__(item)

        def __getslice__(self, i, j):
                return self.value.__getslice__(i, j)

        def __enter__(self):
                return self.value.__enter__()

        def __exit__(self, exc_type, exc_value, traceback):
                self.value.__exit__(exc_type, exc_value, traceback)


def islazy(_object):
        return isinstance(_object, LazyPeek)

def lazytype(_object):
        if isinstance(_object, LazyPeek):
                return _object.type
        else:
                return type(_object)

def lazyvalue(_object, deep=False):
        if isinstance(_object, LazyPeek):
                return _object.peek(deep)
        else:
                return _object



class FileStore(LazyStore):
        __slots__ = ('resource', 'temporary')
        def __init__(self, storables, resource, mode=None, **kwargs):
                LazyStore.__init__(self, storables, mode=mode, **kwargs)
                self.resource = resource
                self.temporary = None
                file_exists = os.path.isfile(resource)
                if self.writes(mode) and file_exists:
                        dirname, basename = os.path.split(resource)
                        if basename[0] != '.':
                                basename = '.' + basename
                        for c in 'pqrstuvwxyz0123456789':
                                temporary = os.path.join(dirname, '{}.sw{}'.format(basename, c))
                                exists = os.path.isfile(temporary)
                                if not exists:  break
                        if exists:
                                import tempfile
                                f, temporary = tempfile.mkstemp()
                                os.close(f)
                        if self.verbose:
                                print('flushing into temporary file: {}'.format(temporary))
                        self.temporary = temporary
                        self.open_args = (temporary, )
                elif self.writes(mode) or file_exists:
                        self.open_args = (resource, )
                else:
                        # reading a missing file
                        try:
                                raise FileNotFoundError(resource)
                        except NameError:
                                import errno
                                raise OSError(errno.ENOENT, 'file not found: {}'.format(resource))
                self.open()

        def writes(self, mode):
                return mode in 'aw'

        def close(self):
                LazyStore.close(self)
                if self.temporary:
                        shutil.move(self.temporary, self.resource)
                        self.temporary = None

        def __del__(self):
                # if the caller forgot to call `close` (e.g. on hitting an unexpected error)
                if self.temporary:
                        try:
                                os.unlink(self.temporary)
                        except (KeyboardInterrupt, SystemExit):
                                raise
                        except:
                                pass


def _issubclass(a, b):
        try:
                return issubclass(a, b)
        except TypeError:
                return False


# overwrites
def peek_assoc(s, c, *args, **kwargs):
        # fully peeks the first elements
        items = [ lazyvalue(a) for a in generic.peek_assoc(s, c, *args, **kwargs) ]
        return [ (lazyvalue(a, deep=True), b) for a, b in items ]

import collections
_overwrites = {
        list:   generic.assoc_to_list,
        tuple:  list,
        set:    list,
        frozenset:      list,
        dict:   None,
        collections.deque:      list,
        collections.OrderedDict:        None,
        }
try:
        import pandas
except ImportError:
        pass
else:
        _overwrites[pandas.Index] = list

def _wrap(f):
        return lambda s, c, *args, **kwargs: f(peek_assoc(s, c, *args, **kwargs))

def _peek(_type):
        strategy = _overwrites[_type]
        if strategy is None:
                return _type
        elif isinstance(strategy, type):
                f = _peek(strategy)
                return lambda a: _type(f(a))
        elif callable(strategy):
                return strategy
        else:
                raise ValueError

# update `seq_storables`
for _storable in generic.seq_storables:
        if _storable.handlers[1:]:
                import warnings
                warnings.warn('multiple handlers for storable: {}'.format(_storable.python_type),
                        DeprecationWarning) # raw.lazy may be outdated
                continue
        try:
                _new_peek = _wrap(_peek(_storable.python_type))
        except KeyError:
                import warnings
                warnings.warn('unsupported sequence storable: {}'.format(_storable.python_type),
                        DeprecationWarning) # raw.lazy may be outdated
                continue
        _storable.handlers[0].peek = _new_peek

