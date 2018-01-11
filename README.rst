
RWA-python
==========

**RWA-python** serializes Python datatypes and stores them in HDF5 files.

Code example::

	from rwa import HDF5Store

	class CustomClass(object):
		__slots__ = 'my_slot'
		def __init__(self, slot=None):
			self.my_slot = slot

	# make any complex construct
	any_object = CustomClass((CustomClass(5), dict(a=1)))

	# serialize
	hdf5 = HDF5Store('my_file.h5', 'w')
	hdf5.poke('my object', any_object)
	hdf5.close()

	# deserialize
	hdf5 = HDF5Store('my_file.h5', 'r')
	reloaded_object = hdf5.peek('my object')
	hdf5.close()


With Python3, **RWA-python** serialization is fully automatic.
The library generates serialization schemes for most custom types.
When deserializing objects, it also looks for and loads the modules where the corresponding types are defined.

With Python2, the library requires explicit definitions if many cases.
Current recommendations include using new style classes (deriving new classes from `object`) 
and defining the `__slots__` class variable.


Installation
------------

You will need Python >= 2.7 or >= 3.5.
::

	pip install --user rwa-python

``pip install`` will install some Python dependencies if missing, but you may still need to install the `HDF5 reference library <https://support.hdfgroup.org/downloads/index.html>`_.

