
import rethinkdb as r
from contextlib import contextmanager
from rethinkdb.errors import RqlDriverError

try:
    from queue import Queue, Empty
except ImportError:
    from Queue import Queue, Empty


from .utils import Counter


class Connection(object):
    def __init__(self, db='test', host='localhost', port=28015, auth_key=''):
        self.db = db
        self.host = host
        self.port = port
        self.auth_key = auth_key
        self._conn = None

    def connect(self):
        self._conn = r.connect(host=self.host, port=self.port,
                               auth_key=self.auth_key, db=self.db)

    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None

    @property
    def conn(self):
        if not self._conn:
            self.connect()
        return self._conn


class ConnectionPool(object):
    def __init__(self, max_connections=5):
        self.q = Queue()
        self.max_connections = max_connections
        self._created_connections = Counter()
        self._next_connection_args_num = Counter(mod_value=1)
        self.connection_class = Connection
        self.connection_kwargs = [{}]

    def configure(self, max_connections=5, multi_connection_kwargs=None, **connection_kwargs):
        self.max_connections = max_connections
        if multi_connection_kwargs:
            self.connection_kwargs = multi_connection_kwargs
            self._next_connection_args_num = Counter(mod_value=len(multi_connection_kwargs))
        else:
            self.connection_kwargs = [connection_kwargs]

    def get(self):
        conn = None

        try:
            # Iterate through Queue trying to return an open connection, leave loop when Queue is empty
            while True:
                try:
                    conn = self.q.get_nowait()
                    conn.check_open()
                    return conn
                except RqlDriverError:
                    # Connection is not open
                    del conn
                    self._created_connections.decr()
        except Empty:
            # Create connection if possible and return that
            if self._created_connections.current() < self.max_connections:
                # Attempt connections until we run out of hosts
                for attempts in range(len(self.connection_kwargs)):
                    conn = self.connection_class(**self._next_connection_args()).conn
                    if not conn.is_open():
                        continue
                    self._created_connections.incr()
                    return conn
                raise RqlDriverError("Unable to connect")
            raise

    def put(self, connection):
        self.q.put(connection)

    def created(self):
        return self._created_connections.current()

    def _next_connection_args(self):
        """
        Get the next set of connection arguments
        :return: Dictionary of connection arguments for passing into a Connection
        """
        next_args_num = self._next_connection_args_num.current()
        self._next_connection_args_num.incr()
        return self.connection_kwargs[next_args_num]


pool = ConnectionPool()


@contextmanager
def get_conn():
    conn = pool.get()
    try:
        yield conn
    finally:
        pool.put(conn)

