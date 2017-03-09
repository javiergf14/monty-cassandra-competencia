from cassandra.cluster import Cluster
from cassandra.protocol import ConfigurationException


class CassandraLogic:
    """Cassandra Logic class for manipulating, querying and inserting data.

       Args:
           contact_point (str): A contact point addresses a Cassandra cluster.
           keyspace (str): Name of the key space.
           create_keyspace (boolean): Whether to create a new key space or to just connect the session.

       Attributes:
           keyspace (str): Name of the key space.
           string_set (set): Columns names that are string type (the rest will be int by default).
           session (Session object): A session holds connections to a Cassandra cluster, allowing it to be queried.
       """

    def __init__(self, contact_point, keyspace, create_keyspace):
        self.contact_point = contact_point
        self.keyspace = keyspace
        self.string_set = set()
        self.string_set.add("pais")
        self.string_set.add("codigo_postal")
        self.string_set.add("pais_destino")
        self.string_set.add("competidor")
        self.string_set.add("divisa")
        self.string_set.add("modo_entrega")
        self.string_set.add("canal_captacion")
        self.string_set.add("user")
        self.string_set.add("ciudad")

        # Connect the application to the Cassandra cluster
        cluster = Cluster([contact_point], port=9042, cql_version='3.4.4')
        self.session = cluster.connect() if create_keyspace else cluster.connect(self.keyspace)

    def connect_keyspace(self):
        """Connect the Cassandra cluster to a certain key space.

            Returns:
                New Cassandra Logic instance.

        """
        return CassandraLogic(self.contact_point, self.keyspace, False)

    def drop_and_create_keyspace(self):
        """Drop and create new key space.

            First, it tries to drop the key space and then it creates the key space.
            If the drop fails, it creates directly the key space.

            Returns:
                New Cassandra Logic instance
        """
        try:
            self.session.execute("Drop KEYSPACE {}".format(self.keyspace))
            self._create_keyspace()
        except ConfigurationException:
            self._create_keyspace()

        return self.connect_keyspace()

    def _create_keyspace(self):
        """Create new key space.

            Default parameters:
                replication strategy: SimpleStrategy
                replication factor: 3
        """
        create_keyspace_query = "CREATE KEYSPACE " + self.keyspace + " WITH replication = " \
                                                                     "{'class': 'SimpleStrategy', " \
                                                                     "'replication_factor' : 3}"
        self.session.execute(create_keyspace_query)

    def drop_and_create_table(self, table_name):
        """Drop and create a table.

           First, it tries to drop the table and then it creates the table.
           If the drop fails, it creates directly the table.

           Args:
               table_name (str): name of the table.
        """

        try:
            self.session.execute("DROP TABLE {}".format(table_name))
            self._create_table(table_name)
            self._create_functions()
        except:
            self._create_table(table_name)
            self._create_functions()

    def _create_table(self, table_name):
        """Create a table with a certain format.

            Args:
                table_name (str): name of the table.
        """
        # create_table_query = "CREATE TABLE " + table_name + "(" \
        #                      + "competidor text, " \
        #                      + "pais text, "\
        #                      + "codigo_postal text, " \
        #                      + "pais_destino text, "\
        #                      + "divisa text, "\
        #                      + "importe varint, "\
        #                      + "modo_entrega text, "\
        #                      + "canal_captacion text, "\
        #                      + "user text, "\
        #                      + "timestamp varint, "\
        #                      + "comision varint, "\
        #                      + "tasa_cambio varint, "\
        #                      + "PRIMARY KEY (competidor, pais, codigo_postal, pais_destino, divisa, importe, modo_entrega) );"

        create_table_query = "CREATE TABLE " + table_name + "(" \
                             + "ciudad text, "\
                             + "pais_destino text, " \
                             + "divisa text, " \
                             + "competidor text, " \
                             + "comision double, " \
                             + "tasa_cambio double, " \
                             + "timestamp double, " \
                             + "PRIMARY KEY (ciudad, pais_destino, divisa, competidor) );"

        self.session.execute(create_table_query)

    def _create_functions(self):
        """Create User Defined Functions

           Function 1:
               Calculate the importe_destino based on the comision, importe and tasa_cambio.
               Formula: importe_destino = importe*tasa_cambio - comision
        """
        self.session.execute("""
            CREATE FUNCTION importe_destino(comision double, tasa_cambio double, importe double)
                RETURNS NULL ON NULL INPUT
                RETURNS double
                LANGUAGE java
                AS '
                return importe*tasa_cambio-comision;'
        """)

    def insert_data(self, table_name, column_names, column_values):
        """Insert a row in a table

           Notice we do not need to include all the columns names nor columns values due to the flexible schema.
           The column values are integer by default, except if they are included in the string_set.

           Args:
               table_name (str): name of the table.
               column_names (list): name of the columns to insert.
               column_values (list): values of the columns to insert.
        """
        insert_data_query = "INSERT INTO {} (".format(table_name)
        insert_data_query += ",".join(column_names)
        insert_data_query += ") VALUES("
        processed_values = ["'"+c+"'" if column_names[cont] in self.string_set else c for cont, c in enumerate(column_values)]
        insert_data_query += ",".join(processed_values)
        insert_data_query += ");"

        self.session.execute(insert_data_query)

    def select_all(self, table_name):
        """Select all the information of a table

           Args:
               table_name (str): name of the table.

            Returns:
                All the rows of the table.
        """
        results = self.session.execute("SELECT * FROM {}".format(table_name))
        rows = []
        for res in results:
            row = []
            for r in res:
                row.append(str(r))
            rows.append(row)
        return rows