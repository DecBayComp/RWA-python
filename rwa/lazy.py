
from .generic import GenericStore
from threading import Lock
import os
import shutil


class LazyStore(GenericStore):

	__slots__ = ('handle', 'lazy', '_lock', 'open_args', 'open_kwargs')

	def __init__(self, storables, verbose=False, **kwargs):
		GenericStore.__init__(self, storables, verbose)
		self.handle = None
		self.lazy = True
		self._lock = Lock()
		self.open_args = ()
		self.open_kwargs = kwargs

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

	def peek(self, objname, container, lazy=None):
		if lazy is None:
			return GenericStore.peek(self, objname, container)
		else:
			try:
				past_value, self.lazy = self.lazy, lazy
				return GenericStore.peek(self, objname, container)
			finally:
				self.lazy = past_value

	def peekStorable(self, storable, record):
		if self.lazy:
			return LazyPeek(self, storable, record)
		else:
			return GenericStore.peekStorable(self, storable, record)

	def poke(self, objname, obj, record, visited=None):
		GenericStore.poke(self, objname, lazyvalue(obj), record, visited)

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
	__slots__ = ('storable', 'store', 'locator')

	def __init__(self, store, storable, container):
		self.storable = storable
		self.store = store
		self.locator = store.locator(container)

	def peek(self, deep=False, block=True):
		if not self.store.lock(block):
			return
		try:
			previous, self.store.lazy = self.store.lazy, not deep
			try:
				return GenericStore.peekStorable(self.store, self.storable,
					self.store.container(self.locator))
			finally:
				self.store.lazy = previous
		finally:
			self.store.release()

	def deep(self):
		return self.peek(True)

	def shallow(self):
		return self.peek()

	#@property
	#def value(self):
	#	return self.deep()

	@property
	def type(self):
		return self.storable.python_type


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
		if self.writes(mode) and os.path.isfile(resource):
			dirname, basename = os.path.split(resource)
			if basename[0] != '.':
				basename = '.' + basename
			for c in 'pqrstuvwxyz0123456789':
				temporary = os.path.join(dirname, '{}.sw{}'.format(basename, c))
				exists = os.path.isfile(temporary)
				if not exists:	break
			if exists:
				import tempfile
				f, temporary = tempfile.mkstemp()
				os.close(f)
			self.temporary = temporary
			self.open_args = (temporary, )
		else:
			self.open_args = (resource, )
		self.open()

	def writes(self, mode):
		return mode == 'w'
	
	def close(self):
		LazyStore.close(self)
		if self.temporary:
			shutil.move(self.temporary, self.resource)
			self.temporary = None

