import rethinkdb as r


class ObjectHandler(object):
    def __init__(self, model_cls, query=None):
        self.model_cls = model_cls
        self.query = query or r.table(model_cls._table)

    def __getattr__(self, name):
        return getattr(self.query, name)

    def all(self):
        return ObjectSet(self, self.query)

    def create(self, **kwargs):
        obj = self.model_cls(**kwargs)
        obj.save()
        return obj

    def get(self, id_=None, **kwargs):
        if id_:
            try:
                doc = self.query.get(id_).run()
            except AttributeError:
                # self.query has a get_all applied, cannot call get
                kwargs.update(id=id_)
            else:
                if doc is not None:
                    return self._wrap(doc)
                return None
        docs = self.query.filter(kwargs).limit(1).run()
        try:
            return self._wrap(list(docs)[0])
        except IndexError:
            return None

    def get_or_create(self, id_=None, **kwargs):
        obj = self.get(id_, **kwargs)
        if obj:
            return obj, False
        return self.create(**kwargs), True

    def filter(self, ids=None, **kwargs):
        if ids:
            try:
                query = self.query.get_all(r.args(ids)).filter(kwargs)
            except AttributeError:
                # self.query already has a get_all applied
                query = (self.query.filter(lambda doc: r.expr(ids).contains(doc['id']))
                                   .filter(kwargs))
        else:
            query = self.query.filter(kwargs)
        return ObjectSet(self, query)

    def count(self):
        return self.query.count().run()

    def _wrap(self, doc):
        obj = self.model_cls()
        # Assign fields this way to skip validation
        obj.fields.__dict__ = doc
        return obj


class ObjectSet(object):
    def __init__(self, object_handler, query):
        self.object_handler = object_handler
        self.query = query
        self.result_cache = None

    def __iter__(self):
        self._fetch_results()
        return iter(self.result_cache)

    def __len__(self):
        self._fetch_results()
        return len(self.result_cache)

    def __getitem__(self, key):
        object_set = self
        try:  # Try treating it like a slice, first
            start = key.start
            stop = key.stop
            if start is not None and stop is not None:
                object_set = ObjectSet(self.object_handler, self.query.slice(start, stop))
            elif start is not None:
                object_set = ObjectSet(self.object_handler, self.query.slice(start))
            elif stop is not None:
                object_set = ObjectSet(self.object_handler, self.query.limit(stop))
            return object_set  # Return a filtered set, since we've just sliced the query
        except AttributeError:  # Must not be a slice!
            # It's an integer (or so we assume), so just get that single item
            return ObjectSet(self.object_handler, self.query.nth(key)).iterator().next()

    def count(self):
        return self.query.count().run()

    def iterator(self):
        results = self.query.run()
        for doc in results:
            yield self.object_handler._wrap(doc)

    def _fetch_results(self):
        if self.result_cache is None:
            self.result_cache = list(self.iterator())
