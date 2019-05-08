import warnings
import base64
import json
from dstools.pipeline.products import Product
from dstools.pipeline.tasks import Task
from dstools.sql import infer

from psycopg2 import sql

CONN = None


class PostgresConnectionMixin:
    def _set_conn(self, conn):
        self._conn = conn

    def _get_conn(self):
        if self._conn is not None:
            return self._conn
        elif CONN is not None:
            return CONN
        else:
            ValueError('You have to either pass a connection object '
                       'in the constructor or set postgres.CONN to '
                       'a connection to be used by all postgres objects')


class JSONSerializer:
    @staticmethod
    def serialize(metadata):
        return json.dumps(metadata)

    @staticmethod
    def deserialize(metadata_str):
        return json.loads(metadata_str)


class Base64Serializer:
    @staticmethod
    def serialize(metadata):
        json_str = json.dumps(metadata)
        return base64.encodebytes(json_str.encode('utf-8')).decode('utf-8')

    @staticmethod
    def deserialize(metadata_str):
        bytes_ = metadata_str.encode('utf-8')
        metadata = json.loads(base64.decodebytes(bytes_).decode('utf-8'))
        return metadata


class PostgresRelation(PostgresConnectionMixin, Product):
    def __init__(self, identifier, conn=None,
                 metadata_serializer=Base64Serializer):
        if len(identifier) != 3:
            raise ValueError('identifier must have 3 elements, '
                             f'got: {len(identifier)}')

        self._set_conn(conn)

        # check if a valid conn is available before moving forward
        self._get_conn()

        self.metadata_serializer = metadata_serializer

        # overriding superclass init since we need a PostgresIdentifier here
        self._identifier = PostgresIdentifier(*identifier)
        self.tests, self.checks = [], []
        self.did_download_metadata = False
        self.task = None

    def fetch_metadata(self):
        # https://stackoverflow.com/a/11494353/709975
        query = """
        SELECT description
        FROM pg_description
        JOIN pg_class ON pg_description.objoid = pg_class.oid
        JOIN pg_namespace ON pg_class.relnamespace = pg_namespace.oid
        WHERE nspname = %(schema)s
        AND relname = %(name)s
        """
        cur = self._get_conn().cursor()
        cur.execute(query, dict(schema=self.identifier.schema,
                                name=self.identifier.name))
        metadata = cur.fetchone()
        cur.close()

        # no metadata saved
        if metadata is None:
            return None
        else:
            return self.metadata_serializer.deserialize(metadata[0])

        # TODO: also check if metadata  does not give any parsing errors,
        # if yes, also return a dict with None values, and maybe emit a warn

    def save_metadata(self):
        metadata = self.metadata_serializer.serialize(self.metadata)

        schema = sql.Identifier(self.identifier.schema)
        name = sql.Identifier(self.identifier.name)

        if self.identifier.kind == PostgresIdentifier.TABLE:
            query = (sql.SQL("COMMENT ON TABLE {}.{} IS %(metadata)s;")
                     .format(schema, name))
        else:
            query = (sql.SQL("COMMENT ON VIEW {}.{} IS %(metadata)s;")
                     .format(schema, name))

        cur = self._get_conn().cursor()
        cur.execute(query, dict(metadata=metadata))
        self._get_conn().commit()
        cur.close()

    def __repr__(self):
        id_ = self.identifier
        return f'PG{id_.kind.capitalize()}: {id_.schema}.{id_.name}'

    def __str__(self):
        id_ = self.identifier
        return f'{id_.schema}.{id_.name}'

    def exists(self):
        # https://stackoverflow.com/a/24089729/709975
        query = """
        SELECT EXISTS (
            SELECT 1
            FROM   pg_catalog.pg_class c
            JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE  n.nspname = %(schema)s
            AND    c.relname = %(name)s
        );
        """

        cur = self._get_conn().cursor()
        cur.execute(query, dict(schema=self.identifier.schema,
                                name=self.identifier.name))
        exists = cur.fetchone()[0]
        cur.close()
        return exists

    @property
    def name(self):
        return self.identifier.name

    @property
    def schema(self):
        return self.identifier.schema


class PostgresIdentifier:
    TABLE = 'table'
    VIEW = 'view'

    def __init__(self, schema, name, kind):
        if len(name) > 63:
            url = ('https://www.postgresql.org/docs/current/'
                   'sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS')
            raise ValueError(f'"{name}" exceeds maximum length of 63 '
                             f' (length is {len(name)}), '
                             f'see: {url}')

        if kind not in [self.TABLE, self.VIEW]:
            raise ValueError('kind must be one of ["view", "table"] '
                             f'got "{kind}"')

        self.kind = kind
        self.schema = schema
        self.name = name

    def __str__(self):
        return f'{self.schema}.{self.name}'

    def __repr__(self):
        return f'{self.schema}.{self.name} (PG{self.kind.capitalize()})'

    def __call__(self):
        return self


class PostgresScript(PostgresConnectionMixin, Task):
    """A tasks represented by a SQL script run agains a Postgres database
    """

    def __init__(self, code, product, dag, conn=None, name=None, params={}):
        super().__init__(code, product, dag, name, params)

        self._set_conn(conn)

        # check if a valid conn is available before moving forward
        self._get_conn()
        self._validate()

    def _validate(self):
        infered_relations = infer.created_relations(self.source_code)

        if not infered_relations:
            warnings.warn('It seems like your code will not create any '
                          'TABLES or VIEWS but your product is '
                          f'{self.product}')
        elif len(infered_relations) > 1:
            warnings.warn('It seems like your code will create create more '
                          'than one TABLES or VIEWS but you only declared '
                          f' one product: {self.product}')
        else:
            schema, name, kind = infered_relations[0]
            id_ = self.product.identifier

            if ((schema != id_.schema) or (name != id_.name)
                    or (kind != id_.kind)):
                warnings.warn('It seems like your code will create create a '
                              f'{kind} in {schema}.{name} but your product '
                              f'did not match: {self.product}')

    def run(self):
        cursor = self._get_conn().cursor()
        cursor.execute(self.source_code)
        self._get_conn().commit()

        self.product.check()
        self.product.test()
