
import os
import six

import h5py

from numpy import string_, ndarray#, MaskedArray
import tempfile
import itertools
from .storable import *
from .generic import *
from .lazy import FileStore
from .sequence import *


# to_string variants
if six.PY3:
	def from_unicode(s): return s
	def from_bytes(b): return b.decode('utf-8')
	def to_str(s):
		if isinstance(s, str):
			return s
		else:
			return from_bytes(s)
	def to_binary(s):
		if isinstance(s, str):
			s = s.encode('utf-8')
		return string_(s)
else:
	import codecs
	def from_unicode(s):
		return codecs.unicode_escape_encode(s)[0]
	def from_bytes(b): return b
	to_str = str
	to_binary = string_

to_attr = string_
from_attr = from_bytes

def native_poke(service, objname, obj, container, visited=None):
	container.create_dataset(objname, data=obj)

def string_poke(service, objname, obj, container, visited=None):
	container.create_dataset(objname, data=to_binary(obj))

def vlen_poke(service, objname, obj, container, visited=None):
	dt = h5py.special_dtype(vlen=type(obj))
	container.create_dataset(objname, data=obj, dtype=dt)

def native_peek(service, container):
	return container[...]

def binary_peek(service, container):
	return container[...].tostring()

def text_peek(service, container):
	return container[...].tostring().decode('utf-8')
	

def mk_vlen_poke(f):
	def poke(service, objname, obj, container, visited=None):
		obj = f(obj)
		dt = h5py.special_dtype(vlen=type(obj))
		container.create_dataset(objname, data=obj, dtype=dt)
	return poke

def mk_native_peek(f):
	def peek(service, container):
		return f(container[...])
	return peek



def _debug(f):
	def printname(name, obj):
		print(obj.name)
		for a in obj.attrs:
			try:
				print(' - {}={}'.format(a, obj.attrs[a]))
			except OSError:
				print(' - {}=(empty)'.format(a))
			except TypeError as e:
				print(' - {}=({})'.format(a, e.args[0]))
	f.visititems(printname)

try:
	from pandas import read_hdf, Series, DataFrame, Panel, SparseSeries
except ImportError:
	pass
else:
	try:
		import tables
	except ImportError as e:
		import warnings
		warnings.warn(e.msg, ImportWarning)

	def copy_hdf(from_table, to_table, name):
		from_table.copy(from_table, to_table, name=name)

	def peek_Pandas(service, from_table):
		fd, tmpfilename = tempfile.mkstemp()
		os.close(fd)
		try:
			to_table = h5py.File(tmpfilename, 'w')
			try:
				copy_hdf(from_table['root'], to_table, 'root')
			finally:
				to_table.close()
			table = read_hdf(tmpfilename, 'root')
		finally:
			os.unlink(tmpfilename)
		return table

	def poke_Pandas(service, objname, obj, to_table, visited=None):
		fd, tmpfilename = tempfile.mkstemp()
		os.close(fd)
		try:
			obj.to_hdf(tmpfilename, 'root')
			from_table = h5py.File(tmpfilename, 'r', libver='latest')
			try:
				copy_hdf(from_table, to_table, objname)
			finally:
				from_table.close()
		finally:
			os.unlink(tmpfilename)
	#_debug(to_table.file)
	pandas_storables += [Storable(Series, handlers=StorableHandler(peek=peek_Pandas, poke=poke_Pandas)), \
		Storable(DataFrame, handlers=StorableHandler(peek=peek_Pandas, poke=poke_Pandas)), \
		Storable(Panel, handlers=StorableHandler(peek=peek_Pandas, poke=poke_Pandas))]


string_storables = [\
	Storable(six.binary_type, key='Python.bytes', \
		handlers=StorableHandler(poke=string_poke, peek=binary_peek)), \
	Storable(six.text_type, key='Python.unicode', \
		handlers=StorableHandler(poke=string_poke, peek=text_peek))]
numpy_storables += [Storable(ndarray, handlers=StorableHandler(poke=native_poke, peek=native_peek))]

class SequenceV2(SequenceHandling):
	def suitable_record_name(self, name):
		return isinstance(name, generic.strtypes + (int, ))
	def to_record_name(self, name):
		if isinstance(name, int):
			name = str(name)
		return name
	def from_record_name(self, name, typestr=None):
		if 'int' in typestr.lower():
			name = int(name)
		return name
	def iter_records(self, store, container):
		return container.keys()
	def suitable_array_element(self, elem):
		return True # let's delegate to `poke_array`
	def poke_array(self, store, name, elemtype, elements, container, visited):
		native_poke(store, name, elements, container, visited=visited)
		return store.getRecord(name, container)
	def peek_array(self, store, elemtype, container):
		#if elemtype is six.text_type:
		#	return text_peek(store, container)
		#elif elemtype is six.binary_type:
		#	return binary_peek(store, container)
		#else:
			return native_peek(store, container)

import copy
_seq_handlers_v2 = SequenceV2().base_handlers()
seq_storables_v2 = []
for _type, _handler in _seq_handlers_v2.items():
	for _storable in seq_storables:
		if _storable.python_type is _type:
			break
	if _storable.python_type is _type:
		_storable = copy.deepcopy(_storable)
		_handler.version = (2,)
		_storable.handlers.append(_handler)
	else:
		_storable = Storable(_type, handlers=_handler)
	seq_storables_v2.append(_storable)


hdf5_storables = itertools.chain(\
	with_type_support(function_storables), \
	string_storables, \
	seq_storables_v2, \
	numpy_storables, \
	sparse_storables, \
	pandas_storables)



# global variable
hdf5_service = StorableService()
for s in hdf5_storables:
	hdf5_service.registerStorable(s)

def hdf5_storable(type_or_storable, *args, **kwargs):
	'''Registers a `Storable` instance in the global service.'''
	if not isinstance(type_or_storable, Storable):
		type_or_storable = default_storable(type_or_storable)
	hdf5_service.registerStorable(type_or_storable, *args, **kwargs)

def hdf5_not_storable(_type, *args, **kwargs):
	'''Tags a type as not serializable.'''
	hdf5_service.registerStorable(not_storable(_type), *args, **kwargs)


hdf5_agnostic_modules = []



class HDF5Store(FileStore):
	'''Store handler for hdf5 files.

	Example::

		hdf5 = HDF5Store(my_file, 'w')
		hdf5.poke('my_object', any_object)
		hdf5.close()

		hdf5 = HDF5Store(my_file, 'r')
		any_object = hdf5.peek('my_object')

	'''
	__slots__ = ()

	def __init__(self, resource, mode='auto', verbose=False, **kwargs):
		FileStore.__init__(self, hdf5_service, resource, mode=mode, verbose=verbose, **kwargs)
		self.lazy = False # for backward compatibility

	def writes(self, mode):
		return mode in ('w', 'auto')

	def __open__(self, resource, mode='auto'):
		if isinstance(resource, h5py.File): # either h5py.File or tables.File
			return resource
		if mode is 'auto':
			if os.path.isfile(resource):
				return h5py.File(resource, 'r', libver='latest')
			else:
				return h5py.File(resource, 'w', libver='latest')
		try:
			return h5py.File(resource, mode)
		except IOError as e:
			if e.args[0] == 'Unable to open file (File signature not found)':
				try:
					raise FileNotFoundError(resource)
				except NameError:
					raise OSError('file not found: {}'.format(resource))

	# backward compatibility property
	@property
	def store(self):
		return self.handle

	@store.setter
	def store(self, store):
		self.handle = store

	#def strRecord(self, record, container):
	#	return to_str(record)

	def formatRecordName(self, objname):
		return six.b(objname) if isinstance(objname, six.text_type) else objname

	def newContainer(self, objname, obj, container):
		group = container.create_group(objname)
		return group

	def getRecord(self, objname, container):
		return container[objname]

	def getRecordAttr(self, attr, record):
		if attr in record.attrs:
			#print(('hdf5.getRecordAttr', attr, record.attrs[attr]))
			return from_attr(record.attrs[attr])
		else:
			return None

	def setRecordAttr(self, attr, val, record):
		#record.attrs[attr] = to_attr(val)
		record.attrs.create(attr, to_attr(val))
		#print(('hdf5.setRecordAttr', record.name, attr, record.attrs[attr])) # DEBUG

	def poke(self, objname, obj, container=None, visited=None):
		if container is None:
			container = self.store
		FileStore.poke(self, objname, obj, container, visited=visited)

	def pokeNative(self, objname, obj, container):
		if obj is not None:
			try:
				container.create_dataset(objname, data=obj)
			except:
				#try: self.pokeStorable(default_storable(obj), objname, obj, container)
				raise TypeError('unsupported type {!s} for object {}'.format(\
					obj.__class__, objname))

	def pokeVisited(self, objname, obj, container, existing, visited=None):
		existing_container, existing_objname = existing
		container[objname] = existing_container[existing_objname] # HDF5 hard link

	def peek(self, objname, record=None):
		if record is None:
			record = self.store
		return FileStore.peek(self, objname, record)

	def peekNative(self, record):
		try:
			obj = record[...]
			if obj.shape is ():
				obj = list(obj.flat)[0]
			return obj
		except AttributeError as e:
			#try: self.peekStorable(default_storable(??), container)
			raise AttributeError('hdf5.peekNative', record.name, *e.args)

	def isNativeType(self, obj):
		return None # don't know; should `tryPokeAny` instead

	def tryPokeAny(self, objname, obj, record, visited=None):
		try:
			self.pokeNative(objname, obj, record)
		except (KeyboardInterrupt, SystemExit):
			raise
		except:
			if six.PY2:
				raise
			_type = type(obj)
			storable = self.defaultStorable(_type, agnostic=self.isAgnostic(_type))
			self.pokeStorable(storable, objname, obj, record, visited=visited)

	def isAgnostic(self, storable_type):
		modules = []
		path = ''
		for submodule in storable_type.__module__.split('.'):
			if path:
				path = '.'.join((path, submodule))
			else:
				path = submodule
			modules.append(path)
		return any([ m in hdf5_agnostic_modules for m in modules ])

	def container(self, name):
		return self.store[name]

	def locator(self, record):
		return record.name

