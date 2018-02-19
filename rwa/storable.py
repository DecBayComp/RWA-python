
from copy import deepcopy
import six
from warnings import warn

class ConflictingVersionWarning(Warning):
	pass


class StorableHandler(object):
	'''Defines how to store an object of the class identified by `_parent`.

	Attributes:

		version (tuple): version number.

		exposes (iterable): list of attributes to get/set.

		peek (callable): read object from store.

		poke (callable): write object in store.

		_parent (Storable): storable instance this handler is associated to.

	'''
	__slots__ = ('version', 'exposes', 'poke', 'peek', '_parent')

	@property
	def python_type(self):
		if self._parent is None:
			raise RuntimeError('corrupted handlers in Storable')
		else:	return self._parent.python_type

	@property
	def storable_type(self):
		if self._parent is None:
			raise RuntimeError('corrupted handlers in Storable')
		else:	return self._parent.storable_type

	def __init__(self, version=None, exposes={}, peek=None, poke=None):
		if version is None:
			version=(1,)
		self.version = version
		self.exposes = exposes
		self._parent = None
		self.peek = peek
		self.poke = poke



class Storable(object):
	'''Describes a storable class.

	Attributes:

		python_type (type): type of the object to serialize.

		storable_type (str): unique identification string.

		_handlers (list): list of handlers.

	'''
	__slots__ = ('python_type', 'storable_type', '_handlers')

	@property
	def handlers(self):
		return self._handlers

	@handlers.setter
	def handlers(self, handlers): # in PY2, setters work only in new style classes
		#if not isinstance(handlers, list):
		#	handlers = [handlers]
		for h in handlers:
			h._parent = self
		self._handlers = handlers

	def __init__(self, python_type, key=None, handlers=[]):
		self.python_type = python_type
		self.storable_type = key
		self._handlers = [] # PY2?
		if not isinstance(handlers, list):
			handlers = [handlers]
		self.handlers = handlers

	def hasVersion(self, version):
		return version in [ h.version for h in self.handlers ]
		
	def asVersion(self, version=None):
		handler = None
		if version is None:
			for h in self.handlers:
				if handler is None or handler.version < h.version:
					handler = h
		else:
			version = to_version(version)
			for h in self.handlers:
				if h.version == version:
					handler = h
					break
			if handler is None:
				raise KeyError('no such version number: {}'.format(version))
		return handler

	def poke(self, *vargs, **kwargs):
		self.asVersion(kwargs.pop('version', None)).poke(*vargs, **kwargs)

	def peek(self, *vargs, **kwargs):
		return self.asVersion(kwargs.pop('version', None)).peek(*vargs, **kwargs)


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

		by_storable_type (dict): dictionnary of storable instances with identification strings as keys.

	'''
	__slots__ = ('by_python_type', 'by_storable_type') # what about native_type?

	def __init__(self):
		self.by_python_type = {}
		self.by_storable_type = {}

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
			if pokes and self.hasPythonType(storable.python_type):
				raise TypeError('conflicting instances', storable.python_type)
			existing = deepcopy(storable)
			existing._handlers = []
		# .. and add the other/new handlers
		for h in storable.handlers:
			h._parent = existing # PY2 requires to use `_handlers` instead of `handlers`
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

	def byPythonType(self, t):
		if isinstance(t, type):
			try:
				return self.by_python_type[t]
			except KeyError:
				return None
		else:
			raise TypeError
			try:
				return self.by_python_type[type(t)]
			except KeyError:
				try:
					return self.by_python_type[t.__class__]
				except (AttributeError, KeyError):
					return None

	def hasPythonType(self, t):
		if isinstance(t, type):
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

	def byPythonType(self, t):
		return self.storables.byPythonType(t)

	def hasPythonType(self, t):
		return self.storables.hasPythonType(t)

	def byStorableType(self, t):
		return self.storables.byStorableType(t)

	def hasStorableType(self, t):
		return self.storables.hasStorableType(t)

	def registerStorable(self, storable, **kwargs):
		self.storables.registerStorable(storable, **kwargs)

	def peek(self, objname, container):
		'''Reads from a container.

		Arguments:

			objname (str): object name.

			container (any): address of the object in the store.

		Returns:

			any: deserialized object.

		'''
		raise NotImplementedError('abstract method')

	def poke(self, objname, obj, container, visited=None):
		'''Writes in a container.

		Arguments:

			objname (str): object name.

			obj (any): object to serialize.

			container (any): address of the object in the store.

			visited (dict): already seriablized objects.

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


