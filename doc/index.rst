
RWA-python documentation
========================

|rwa-python| serializes Python datatypes and stores them in HDF5 files.

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

With |py3|, |rwa-python| serialization is fully automatic for types with *__slots__* defined or such that the *__init__* constructor does not require any input argument.

The library generates serialization schemes for most custom types.
When deserializing objects, it also looks for and loads the modules where the corresponding types are defined.

If |rwa-python| complains about a type that cannot be serialized, a partial fix consists of ignoring this datatype:

.. code-block:: python

	hdf5_not_storable(type(unserializable_object))


With |py2|, the library requires explicit definitions in most cases.
In addition, string typing is sometimes problematic. Non-ascii characters should be explicit unicode.


Installation
------------

Python >= 2.7 or >= 3.5 is required.

*pip* should work just fine:

::

	pip install --user rwa-python

*pip install* will install some Python dependencies if missing, but you may still need to install the `HDF5 reference library <https://support.hdfgroup.org/downloads/index.html>`_.


Explicitly supported datatypes
------------------------------

* any datatype supported by `h5py <http://docs.h5py.org/en/stable/faq.html#what-datatypes-are-supported>`_
* *type*
* sequences and collections including *tuple*, *list*, *frozenset*, *set*, *dict*, *namedtuple*, *deque*, *OrderedDict*, *Counter*, *defaultdict* and *memoryview*
* some *pandas* datatypes including *Index*, *Int64Index*, *Float64Index*, *MultiIndex*, *Series*, *DataFrame* and *Panel* (*Panel* is supported only with package *tables* available)
* in *scipy.sparse*, types *bsr_matrix*, *coo_matrix*, *csc_matrix*, *csr_matrix*, *dia_matrix*, *dok_matrix* and *lil_matrix*

*pandas* and *scipy* types with explicit serialization rules may be autoserialized otherwise in |py3| (not tested).
They benefit such rules for backward compatibility.

The following datatypes are implicitly supported with |py3| and are serialized in |py2| with explicit rules:

* in *scipy.spatial*, types *Delaunay*, *ConvexHull* and *Voronoi*

Other datatypes are safely ignored, including built-in and user defined functions, class methods, etc.


Known issues
------------

A |py2|-serialized *scipy.spatial.Delaunay* can be deserialized in |py3| but not conversely.


More about RWA-python
---------------------

.. toctree::
	:maxdepth: 2

	api
	faq


.. |rwa-python| replace:: **RWA-python**
.. |py2| replace:: Python2
.. |py3| replace:: Python3

