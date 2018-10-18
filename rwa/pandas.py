
from __future__ import absolute_import

from .generic import *


try:
    import pandas
except ImportError:
    pandas_storables = []
else:
    rwa_params['pandas.index.force_unicode']   = True
    rwa_params['pandas.columns.force_unicode'] = True
    rwa_params['pandas.categories.force_unicode'] = True

    _unicode = lambda _s: _s.decode('utf-8') if isinstance(_s, bytes) else _s
    def _map(f, seq):
        return type(seq)([ f(a) for a in seq ])
    def _fmap(f):
        return lambda seq: _map(f, seq)
    def _map2(f, seq):
        return _map(_fmap(f), seq)

    def unicode_index(peek, attrs=None):
        """Helper for converting the `name` attribute (and others) of indices into unicode."""
        attrs = set(attrs) if attrs else set()
        attrs.add('name')
        def _peek_index(*args, **kwargs):
            force_unicode = kwargs.pop('force_unicode', None)
            index = peek(*args, **kwargs)
            if force_unicode:
                for attr in attrs:
                    s = getattr(index, attr)
                    if isinstance(s, strtypes):
                        setattr(index, attr, _unicode(s))
                    elif s is not None:
                        setattr(index, attr, _map(_unicode, s))
            return index
        return _peek_index

    def unicode_categories(peek, attrs=None):
        """Helper for converting the `categories` attribute (and others) of categorical datatypes
        into unicode."""
        attrs = set(attrs) if attrs else set()
        attrs.add('categories')
        def _peek_categorical(*args, **kwargs):
            force_unicode = kwargs.pop('force_unicode', None)
            cat = peek(*args, **kwargs)
            if force_unicode:
                for attr in attrs:
                    s = getattr(cat, attr)
                    if isinstance(s, strtypes):
                        setattr(cat, attr, _unicode(s))
                    elif s is not None:
                        setattr(cat, attr, _map(_unicode, s))
            return cat
        return _peek_categorical

    poke_index = poke(['data', 'name'])
    def poke_index(service, ixname, ix, parent_container, visited=None, _stack=None):
        container = service.newContainer(ixname, ix, parent_container)
        service.poke('data', ix.tolist(), container, visited=visited, _stack=_stack)
        service.poke('name', ix.name, container, visited=visited, _stack=_stack)

    def peek_index(service, container, _stack=None, force_unicode=None):
        try:
            data = service.peek('data', container, _stack=_stack)
        except KeyError:
            # try loading it as a generic sequence (backward compatibility)
            data = service.byPythonType(list, True).peek(service, container, _stack=_stack)
        try:
            name = service.peek('name', container, _stack=_stack)
        except (SystemExit, KeyboardInterrupt):
            raise
        except:
            name = None
        if force_unicode:
            data = _map(_unicode, data)
            if name is not None:
                if isinstance(name, strtypes):
                    name = _unicode(name)
                else:
                    # is this possible?
                    name = _map(_unicode, name)
        return pandas.Index(data, name=name)

    def peek_numerical_index(init=pandas.Index, func=None):
        """
        Peek factory for Pandas numerical indices.
        """
        if func is None:
            _peek_index = peek_with_kwargs(init, ['data'])
        else:
            def _peek_index(service, container, _stack=None):
                data = func(service.peek('data', container, _stack=_stack))
                try:
                    name = service.peek('name', container, _stack=_stack)
                except (SystemExit, KeyboardInterrupt):
                    raise
                except:
                    name = None
                return init(data, name=name)
        return unicode_index(_peek_index)

    def peek_multiindex(service, container, _stack=None, force_unicode=None):
        attrs = peek_as_dict(service, container, _stack=_stack)
        if force_unicode:
            try:
                labels = attrs['labels']
            except KeyError:
                pass
            else:
                #attrs['labels'] = \
                #    [ [ _unicode(_label) for _label in _labels ] for _labels in labels ]
                attrs['labels'] = _map2(_unicode, labels)
            try:
                names = attrs['names']
            except KeyError:
                pass
            else:
                #attrs['names'] = [ _unicode(_name) for _name in names ]
                attrs['names'] = _map(_unicode, names)
        return pandas.MultiIndex(**attrs)

    #poke_multiindex = poke(['levels', 'labels', 'names'])
    def poke_multiindex(service, ixname, ix, parent_container, *args, **kwargs):
        """Poke procedure for pandas.MultiIndex.

        Converts all the pandas.core.base.FrozenList into tuples."""
        container = service.newContainer(ixname, ix, parent_container)
        for attrname in ('levels', 'labels'):
            attr = tuple( tuple(item) for item in getattr(ix, attrname) )
            service.poke(attrname, attr, container, *args, **kwargs)
        attrname = 'names'
        attr = tuple( getattr(ix, attrname) )
        service.poke(attrname, attr, container, *args, **kwargs)

    try:
        # UInt64Index is missing in 0.17.1
        pandas_UInt64Index = pandas.UInt64Index
        peek_uint64index = peek_numerical_index(pandas.UInt64Index)
    except AttributeError:
        # convert UInt64 into Int64
        class pandas_UInt64Index(object):
            """
            Placeholder type.
            """
            __slot__ = ()
            pass
        peek_uint64index = peek_numerical_index(pandas.Int64Index, lambda a: a.astype(np.int64))

    poke_rangeindex = poke(['_start', '_stop', '_step', 'name'])
    try:
        # RangeIndex is missing in 0.17.1
        pandas_RangeIndex = pandas.RangeIndex
        peek_rangeindex = peek_with_kwargs(pandas.RangeIndex, ['_start', '_stop', '_step'])
    except AttributeError:
        # convert to Int64Index
        class pandas_RangeIndex(object):
            """
            Placeholder type.
            """
            __slot__ = ()
            pass
        def peek_rangeindex(*args, **kwargs):
            attrs = peek_as_dict(*args, **kwargs)
            return pandas.Int64Index( \
                range( \
                    start=attrs.pop('_start', None), \
                    stop=attrs.pop('_stop', None), \
                    step=attrs.pop('_step', None)), \
                **attrs)

    # some Pandas types have moved several times; force the key
    pandas_storables = [ \
        Storable(pandas.Index, \
         key='Python.pandas.core.index.Index', \
         handlers=StorableHandler(poke=poke_index, peek=peek_index, \
            peek_option='pandas.index.force_unicode')), \
        Storable(pandas.Int64Index, \
         key='Python.pandas.core.index.Int64Index', \
         handlers=StorableHandler(poke=poke_index, \
            peek=peek_numerical_index(pandas.Int64Index), \
            peek_option='pandas.index.force_unicode')), \
        Storable(pandas_UInt64Index, \
         key='Python.pandas.core.index.UInt64Index', \
         handlers=StorableHandler(poke=poke_index, peek=peek_uint64index, \
            peek_option='pandas.index.force_unicode')), \
        Storable(pandas.Float64Index, \
         key='Python.pandas.core.index.Float64Index', \
         handlers=StorableHandler(poke=poke_index, \
            peek=peek_numerical_index(pandas.Float64Index), \
            peek_option='pandas.index.force_unicode')), \
        Storable(pandas_RangeIndex, \
         key='Python.pandas.core.index.RangeIndex', \
         handlers=StorableHandler(poke=poke_rangeindex, peek=unicode_index(peek_rangeindex), \
             peek_option='pandas.index.force_unicode')), \
        Storable(pandas.MultiIndex, \
         key='Python.pandas.core.index.MultiIndex', \
         handlers=StorableHandler(poke=poke_multiindex, peek=peek_multiindex, \
            peek_option='pandas.index.force_unicode'))]

    class DebugWarning(RuntimeWarning):
        pass

    try:
        # CategoricalDtype is missing in 0.17.1

        # tested for versions >= 0.21.0
        pandas_CategoricalDtype = pandas.api.types.CategoricalDtype
        poke_categoricaldtype = poke(['categories','ordered'])
        peek_categoricaldtype = unicode_categories(peek(pandas_CategoricalDtype, \
            ['categories','ordered']))
        pandas_storables.append(Storable(pandas_CategoricalDtype, \
            handlers=StorableHandler(poke=poke_categoricaldtype, peek=peek_categoricaldtype, \
                peek_option='pandas.categories.force_unicode')))

    except AttributeError:
        pass

    poke_categorical = poke(['categories', 'codes', 'ordered'])
    def peek_categorical(*args, **kwargs):
        force_unicode = kwargs.pop('force_unicode', None)
        attrs = peek_as_dict(*args, **kwargs)
        codes = attrs.pop('codes') # `codes` is required
        categories = attrs.pop('categories') # `categories` is required
        if force_unicode:
            categories = _map(_unicode, categories)
        return pandas.Categorical.from_codes(codes, categories, attrs.get('ordered', False))
    pandas_storables.append(Storable(pandas.Categorical, \
        handlers=StorableHandler(poke=poke_categorical, peek=peek_categorical, \
            peek_option='pandas.categories.force_unicode')))

    poke_categoricalindex = poke(['codes','categories','ordered','name'])
    # does not work!
    peek_categoricalindex = unicode_index(unicode_categories( \
        peek_with_kwargs(pandas.CategoricalIndex, ['codes'])))

    pandas_storables.append( \
        Storable(pandas.CategoricalIndex, \
         handlers=StorableHandler(poke=poke_categoricalindex, peek=peek_categoricalindex, \
            peek_option=['pandas.index.force_unicode','pandas.categories.force_unicode'])))


    # `values` is not necessarily the underlying data; may be a coerced representation instead
    poke_series = poke(['data', 'index'])
    peek_series = peek(pandas.Series, ['data', 'index'])
    if True:#six.PY2:
        # `data` is deprecated
        def poke_series(service, sname, s, parent_container, *args, **kwargs):
            container = service.newContainer(sname, s, parent_container)
            service.poke('data', s.values, container, *args, **kwargs)
            service.poke('index', s.index, container, *args, **kwargs)

    # `poke_dataframe` is similar to `poke` but converts part of the dataframe into
    # an ordered dictionnary of columns
    def poke_dataframe(service, dfname, df, parent_container, *args, **kwargs):
        container = service.newContainer(dfname, df, parent_container)
        data = OrderedDict([ (colname, df[colname].values) for colname in df.columns ])
        service.poke('data', data, container, *args, **kwargs)
        service.poke('index', df.index, container, *args, **kwargs)

    _peek_dataframe = peek(pandas.DataFrame, ['data', 'index'])
    def peek_dataframe(service, container, _stack=None, force_unicode=None):
        df = _peek_dataframe(service, container, _stack=_stack)
        if force_unicode:
            df.columns = _map(_unicode, df.columns)
        return df

    pandas_storables += [ \
        Storable(pandas.Series, handlers=StorableHandler(poke=poke_series, peek=peek_series)), \
        Storable(pandas.DataFrame, \
            handlers=StorableHandler(poke=poke_dataframe, peek=peek_dataframe, \
                peek_option='pandas.columns.force_unicode'))]

