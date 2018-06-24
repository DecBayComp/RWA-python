
"""
Storable strategies for contiguous and optionally homogeneous arrays.
"""

from .storable import StorableHandler, format_type
import rwa.generic as generic
import traceback
from collections import deque, Counter, OrderedDict, defaultdict


class SequenceHandling(object):
        __slots__ = ()

        def suitable_record_name(self, name):
                return isinstance(name, generic.strtypes)

        def suitable_array_element(self, elem):
                return False

        def to_record_name(self, name):
                return name

        def from_record_name(self, name, typestr=None):
                return name

        def iter_records(self, store, container):
                """abstract method"""
                raise NotImplementedError

        def new_container(self, store, name, obj, container):
                try:
                        return store.newContainer(name, obj, container)
                except (KeyboardInterrupt, SystemExit):
                        raise
                except:
                        raise ValueError("cannot create new container '{}'".format(name))

        def poke_list_items(self, store, name, _list, container, visited):
                if not _list:
                        return self.new_container(store, name, _list, container)
                # check homogeneity
                _any = next(iter(_list)) # `set` does not support indexing
                if self.suitable_array_element(_any):
                        _type = type(_any)
                        homogeneous = all(type(x) is _type for x in _list) # _list[1:] does not work with `deque`
                else:
                        homogeneous = False
                if homogeneous:
                        try:
                                record = self.poke_homogeneous_list(store, name, _type, _list,
                                        container, visited)
                        except (KeyboardInterrupt, SystemExit):
                                raise
                        except:
                                #raise
                                homogeneous = False
                if not homogeneous:
                        record = self.poke_heterogeneous_list(store, name, _list,
                                container, visited)
                store.setRecordAttr('homogeneous', '1' if homogeneous else '0', record)
                return record

        def poke_list(self, exposes=()):
                def poke(store, name, _list, container, visited=None):
                        assert _list is not None
                        record = self.poke_list_items(store, name, _list, container, visited)
                        for arg in exposes:
                                val = getattr(_list, arg)
                                if val is not None:
                                        #if record is None:
                                        #       record = self.new_container(store, name, _list,
                                        #               container)
                                        store.poke(arg, val, record, visited=visited)
                return poke

        def poke_homogeneous_list(self, store, name, _type, _list, container, visited):
                record = self.poke_array(store, name, _type, list(_list), container, visited)
                if record is not None:
                        store.setRecordAttr('element type', format_type(_type), record)
                return record

        def poke_heterogeneous_list(self, store, name, _list, container, visited):
                sub_container = self.new_container(store, name, _list, container)
                for i, _item in enumerate(_list):
                        store.poke(self.to_record_name(i), _item, sub_container, visited=visited)
                return sub_container

        def poke_array(self, store, name, elemtype, elements, container, visited):
                """abstract method"""
                raise NotImplementedError

        def peek_list_items(self, store, container):
                homogeneous = store.getRecordAttr('homogeneous', container) == '1'
                if homogeneous:
                        return self.peek_homogeneous_list(store, container)
                else:
                        return self.peek_heterogeneous_list(store, container)

        def peek_list(self, factory, exposes=(), **kwargs):
                def peek(store, container):
                        _list = self.peek_list_items(store, container)
                        _list = factory(_list, **kwargs)
                        for arg in exposes:
                                try:
                                        val = store.peek(arg, container)
                                except (KeyboardInterrupt, SystemExit):
                                        raise
                                except:
                                        pass
                                else:
                                        setattr(_list, arg, val)
                        return _list
                return peek

        def peek_homogeneous_list(self, store, container):
                elemtype = store.getRecordAttr('element type', container)
                elems = self.peek_array(store, elemtype, container)
                return list(elems)

        def peek_heterogeneous_list(self, store, container):
                _list = {}
                imax = -1
                for record in self.iter_records(store, container):
                        i = int(record)
                        imax = max(i, imax)
                        _list[i] = store.peek(record, container)
                return [ _list.get(i, None) for i in range(imax+1) ]

        def peek_array(self, store, elemtype, container):
                """abstract method"""
                raise NotImplementedError

        def poke_dict_items(self, store, name, _dict, container, visited, keys_as_record_names=None):
                sub_container = self.new_container(store, name, _dict, container)
                if not _dict:
                        return
                # check homogeneity
                _keys = list(_dict.keys())
                first = _keys[0]
                if keys_as_record_names is not False and self.suitable_record_name(first):
                        _type = type(first)
                        keys_as_record_names = all(isinstance(x, _type) for x in _keys[1:])
                else:
                        keys_as_record_names = False
                if keys_as_record_names:
                        sub_sub_container = self.new_container(store, 'items', _dict, sub_container)
                        for key, value in _dict.items():
                                store.poke(self.to_record_name(key), value, sub_sub_container,
                                        visited=visited)
                        try:
                                _type = store.byPythonType(first).asVersion().storable_type
                        except AttributeError:
                                # native type
                                _type = format_type(_type)
                        store.setRecordAttr('key type', _type, sub_sub_container)
                else:
                        self.poke_list_items(store, 'keys', _keys, sub_container, visited)
                        _values = list(_dict.values())
                        self.poke_list_items(store, 'values', _values, sub_container, visited)
                return sub_container

        def poke_dict(self, exposes=(), keys_as_record_names=None):
                def poke(store, name, _dict, container, visited=None):
                        assert _dict is not None
                        record = self.poke_dict_items(store, name, _dict, container, visited, keys_as_record_names)
                        for arg in exposes:
                                val = getattr(_dict, arg)
                                if val is not None:
                                        #if record is None:
                                        #       record = self.new_container(store, name, _dict,
                                        #               container)
                                        store.poke(arg, val, record, visited=visited)
                return poke

        def peek_dict_items(self, store, container):
                try:
                        _items = store.getRecord(store.formatRecordName('items'), container)
                except KeyError:
                        try:
                                previous_state = store.lazy
                        except AttributeError:
                                lazy_extension = False
                        else:
                                store.lazy = False
                                lazy_extension = True
                        try:
                                _keys = self.peek_list_items(store,
                                        store.getRecord(store.formatRecordName('keys'), container))
                        except KeyError:
                                return ()
                        finally:
                                if lazy_extension:
                                        store.lazy = previous_state
                        _values = self.peek_list_items(store,
                                store.getRecord(store.formatRecordName('values'), container))
                else:
                        _keytype = store.getRecordAttr('key type', _items)
                        _keys = []
                        _values = []
                        for record in self.iter_records(store, _items):
                                _key = self.from_record_name(record, _keytype)
                                _keys.append(_key)
                                _values.append(store.peek(record, _items))
                return zip(_keys, _values)

        def peek_dict(self, factory, exposes=(), **kwargs):
                def peek(store, container):
                        items = self.peek_dict_items(store, container)
                        _dict = factory(items, **kwargs)
                        for arg in exposes:
                                try:
                                        val = store.peek(arg, container)
                                except (KeyboardInterrupt, SystemExit):
                                        raise
                                except:
                                        pass
                                else:
                                        setattr(_dict, arg, val)
                        return _dict
                return peek

        def base_handlers(self):
                def list_handler(factory, *args):
                        return StorableHandler(peek=self.peek_list(factory, exposes=args),
                                poke=self.poke_list(exposes=args))
                def dict_handler(factory, *args, **kwargs):
                        return StorableHandler(peek=self.peek_dict(factory, exposes=args),
                                poke=self.poke_dict(exposes=args, **kwargs))
                def _Counter(zipped):
                        return Counter(**dict(zipped))
                return OrderedDict((
                        (tuple, list_handler(tuple)),
                        (list,  list_handler(list)),
                        (frozenset,     list_handler(frozenset)),
                        (set,   list_handler(set)),
                        (deque, list_handler(deque, 'maxlen')),
                        (dict, dict_handler(dict)),
                        (OrderedDict,   dict_handler(OrderedDict, keys_as_record_names=False)),
                        (defaultdict,   dict_handler(defaultdict, 'default_factory')),
                        (Counter,       dict_handler(_Counter)),
                ))


