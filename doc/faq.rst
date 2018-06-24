
Common mistakes
===============

Types not in sys.path
---------------------

Automatic registration of types works best for types in `sys.path`.

Types not in `sys.path` should be registered with an explicit storable identifier.
Indeed, although they may be serialized, deserialization is likely to fail because |rwa| needs to locate the module where the type is defined.

Typically, types defined in a *__main__* script are not locatable.


Misdefining __slots__
---------------------

When defining ``__slots__`` for a class that inherits from another class with ``__slots__``,
this attribute should not list the members defined in the parent class.

Below follows an example of what should NOT be done:

.. code-block:: python

	class A(object):
		__slots__ = ['A_slot']
	class B(A):
		__slots__ = A.__slots__ + ['B_slot']

This may cause errors in |rwa| that will be difficult to debug.

.. |rwa| replace:: **RWA-python**
