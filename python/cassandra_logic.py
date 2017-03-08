from cassandra.cluster import Cluster
from cassandra.protocol import ConfigurationException


class CassandraLogic:

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

        # Connect the application to the Cassandra cluster
        cluster = Cluster([contact_point])
        self.session = cluster.connect() if create_keyspace else cluster.connect(self.keyspace)

    def connect_keyspace(self):
        return CassandraLogic(self.contact_point, self.keyspace, False)

    def drop_and_create_keyspace(self):
        try:
            self.session.execute("Drop KEYSPACE {}".format(self.keyspace))
            self._create_keyspace()
        except ConfigurationException:
            self._create_keyspace()

        return self.connect_keyspace()

    def _create_keyspace(self):
        create_keyspace_query = "CREATE KEYSPACE " + self.keyspace + " WITH replication = " \
                                                                     "{'class': 'SimpleStrategy', " \
                                                                     "'replication_factor' : 3}"
        self.session.execute(create_keyspace_query)

    def drop_and_create_table(self, table_name):
        try:
            self.session.execute("DROP TABLE {}".format(table_name))
            self._create_table(table_name)
        except:
            self._create_table(table_name)

    def _create_table(self, table_name):

        create_table_query = "CREATE TABLE " + table_name + "(" \
                             + "pais text, "\
                             + "codigo_postal text, " \
                             + "pais_destino text, "\
                             + "competidor text, "\
                             + "divisa text, "\
                             + "importe varint, "\
                             + "modo_entrega text, "\
                             + "canal_captacion text, "\
                             + "user text, "\
                             + "timestamp varint, "\
                             + "comision varint, "\
                             + "tasa_cambio varint, "\
                             + "PRIMARY KEY (pais, codigo_postal, pais_destino, competidor, divisa, importe, modo_entrega) );"

        self.session.execute(create_table_query)

    def insert_data(self, table_name, column_names, column_values):
        insert_data_query = "INSERT INTO {} (".format(table_name)
        insert_data_query += ",".join(column_names)
        insert_data_query += ") VALUES("
        processed_values = ["'"+c+"'" if column_names[cont] in self.string_set else c for cont, c in enumerate(column_values)]
        insert_data_query += ",".join(processed_values)
        insert_data_query += ");"

        self.session.execute(insert_data_query)
