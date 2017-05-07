import psycopg2


class PDBConnector:
    def __init__(self, dbname='postgres', user=None, password=None, host='127.0.0.1', port='5432'):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.connect = self.__connect_to_database()

    def __connect_to_database(self):
        """
        Creates a new database session, returned a new connection instance.
        """
        try:
            return psycopg2.connect('dbname={} user={} password={} host={} port={}' \
                                            .format(self.dbname, self.user, self.password, self.host, self.port))
        except:
            print('Invalid connection parameters or there is no connection to the database.')
            return None

    def write_to_database(self, query, vars=None):
        """Executes the query to the database and commit transaction."""
        if self.connect is not None:
            # allows Python code to execute PostgreSQL command in a database session
            cur = self.connect.cursor()
            cur.execute(query, vars)
            # commit transaction to the database.
            self.connect.commit()
        else:
            raise psycopg2.Error

    def close_connect(self):
        if self.connect is not None:
            self.connect.close()
        else:
            raise psycopg2.Error

    def featch_query_result(self, query):
        """
        Fetch all (remaining) rows of a query result, returning them as a list of tuples. 
        An empty list is returned if there is no more record to fetch.
        :param query: 
        """
        if self.connect is not None:
            # allows Python code to execute PostgreSQL command in a database session
            cur = self.connect.cursor()
            cur.execute(query)
            # fetches and prints all rows of a query result
            print(cur.fetchall())
        else:
            raise psycopg2.Error
