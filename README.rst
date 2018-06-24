
RWA-python
==========

**RWA-python** serializes Python datatypes and stores them in HDF5 files.

Code example
------------

In module *A* referenced in *sys.path*:

.. code-block:: python

	class CustomClass(object):
		def __init__(self, arg=None):
			self.attr = arg

In module *B*:

.. code-block:: python

	from A import CustomClass
	from rwa import HDF5Store

	# make any complex construct
	any_object = CustomClass((CustomClass('a'), dict(b=1)))

	# serialize
	hdf5 = HDF5Store('my_file.h5', 'w')
	hdf5.poke('my object', any_object)
	hdf5.close()

	# deserialize
	hdf5 = HDF5Store('my_file.h5', 'r')
	reloaded_object = hdf5.peek('my object')
	hdf5.close()


Introduction
------------

With Python3, **RWA-python** serialization is fully automatic for types with *__slots__* defined or such that the *__init__* constructor does not require any input argument.

The library generates serialization schemes for most custom types.
When deserializing objects, it also looks for and loads the modules where the corresponding types are defined.

If **RWA-python** complains about a type that cannot be serialized, a partial fix consists of ignoring this datatype:

.. code-block:: python

	hdf5_not_storable(type(unserializable_object))


With Python2, the library requires explicit definitions in most cases.
In addition, string typing is sometimes problematic. Non-ascii characters should be explicit unicode.


Installation
------------

Python >= 2.7 or >= 3.5 is required.

*pip* should work just fine:

::

	pip install --user rwa-python

*pip install* will install some Python dependencies if missing, but you may still need to install the `HDF5 reference library <https://support.hdfgroup.org/downloads/index.html>`_.


See also
--------

**RWA-python** is on `readthedocs <https://rwa-python.readthedocs.io/en/latest/>`_.

