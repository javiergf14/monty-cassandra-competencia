from cassandra.cluster import Cluster
from cassandra.protocol import ConfigurationException
from tables import Table
import Geohash


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

    def __init__(self, contact_point, keyspace, create_keyspace=True):
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
        self.string_set.add("geohash")
        self.tables = {
          "ciudad_query": Table.table_ciudad_scheme(),
          "ciudad_competidor_query": Table.table_ciudad_competidor_scheme(),
          "geohash_query": Table.table_geohash_scheme(),
          "geohash_competidor_query": Table.table_geohash_competidor_scheme(),
          "agente_query": Table.table_agente_scheme(),
          "agente_competidor_query": Table.table_agente_competidor_scheme(),
          "ciudad_timestamp_importe_query": Table.table_ciudad_timestamp_importe_scheme(),
          "ciudad_importe_timestamp_query": Table.table_ciudad_importe_timestamp_scheme(),
          "ciudad_competidor_timestamp_query": Table.table_ciudad_competidor_timestamp_scheme(),
          "ciudad_competidor_importe_timestamp_query": Table.table_ciudad_competidor_importe_timestamp_scheme(),
          "ciudad_importe_nominal_timestamp_query": Table.table_ciudad_importe_nominal_timestamp_scheme(),
          "ciudad_timestamp_importe_nominal_query": Table.table_ciudad_timestamp_importe_nominal_scheme(),
          "ciudad_importe_nominal_y_destino_timestamp_query": Table.table_ciudad_importe_nominal_y_destino_timestamp_scheme()
          }

        # Connect the application to the Cassandra cluster
        cluster = Cluster([contact_point], port=9042, cql_version='3.4.4')
        self.session = cluster.connect() if create_keyspace else cluster.connect(self.keyspace)

    @classmethod
    def from_existing_keyspace(cls, contact_point, keyspace):
        """Connect the Cassandra cluster to a certain key space.

           Returns:
               New Cassandra Logic instance.
        """
        return cls(contact_point, keyspace, False)

    def drop_and_create_keyspace(self):
        """Drop and create new key space.

            First, it tries to drop the key space and then it creates the key space.
            If the drop fails, it creates directly the key space.
    """
        try:
            self.session.execute("Drop KEYSPACE {}".format(self.keyspace))
            self._create_keyspace()
        except ConfigurationException:
            self._create_keyspace()

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

    def drop_and_create_tables(self):
        """Drop and create a table.

           First, it tries to drop the table and then it creates the table.
           If the drop fails, it creates directly the table.

           Args:
               table_name (str): name of the table.
        """
        self._create_functions()
        for table_name, table_query in self.tables.items():
            print(self.tables[table_name])
            try:
                self.session.execute("DROP TABLE {}".format(table_name))
                self.session.execute(table_query)
            except:
                self.session.execute(table_query)

    def _create_functions(self):
        """Create User Defined Functions
        """

        self.session.execute("""
            CREATE FUNCTION nearest_lower_importe(importe_user double, importe_nominal double)
            RETURNS NULL ON NULL INPUT
            RETURNS double
            LANGUAGE java
            AS '
            if(importe_nominal > importe_user){
                return 10000000.0;
            }
            else{
                return importe_user-importe_nominal;
            }'
        """)

        self.session.execute("""
            CREATE FUNCTION nearest_upper_importe(importe_user double, importe_nominal double)
            RETURNS NULL ON NULL INPUT
            RETURNS double
            LANGUAGE java
            AS '
            if(importe_nominal < importe_user){
                return 10000000.0;
            }
            else{
                return importe_nominal-importe_user;
            }'
        """)

    def insert_into_all_tables(self, data):
        column_names = ["ciudad", "pais_destino", "divisa", "competidor", "comision",
                        "tasa_cambio", "timestamp", "lat", "lon", "num_agente", "importe_nominal"]
        column_values = []
        for i in column_names:
            column_values.append(data[i])

        column_names.append("importe_destino")
        importe_destino = 100 * float(data["tasa_cambio"]) - float(data["comision"])
        column_values.append(importe_destino)

        for table_name in self.tables.keys():
            if table_name.startswith("geo"):
                column_names.append("geohash")
                column_values.append(Geohash.encode(float(data['lat']), float(data['lon'])))
                self._insert_data(table_name, column_names, column_values)
                # Remove last items.
                column_names.pop(-1)
                column_values.pop(-1)
            else:
                self._insert_data(table_name, column_names, column_values)

    def _insert_data(self, table_name, column_names, column_values):
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
        processed_values = ["'"+c+"'" if column_names[cont] in self.string_set else str(c) for cont, c in enumerate(column_values)]
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

    def best_tasa(self, table_name, pais_destino, divisa,
                  ciudad=None,
                  num_agente=None,
                  geohash = None,
                  timestamp = None,
                  competidor=None,
                  importe_destino=None,
                  importe_nominal=None,
                  search=None,
                  alt_table=None,
                  mostrar=10):

        # If query data between two timestamps, we need a trick to retrieve the row.
        if timestamp and not importe_destino and importe_nominal and not search:
            sel = 'max(importe_destino)'
        elif timestamp and not importe_destino and not importe_nominal:
            sel = 'max(importe_destino)'
        elif timestamp and importe_nominal and search=='lower':
            sel = 'min(nearest_lower_importe({}, importe_nominal))'.format(importe_nominal)
        elif timestamp and importe_nominal and search=='upper':
            sel = 'min(nearest_upper_importe({}, importe_nominal))'.format(importe_nominal)
        else:
            sel = '*'

        query = "SELECT {} FROM {} ".format(sel, table_name)
        query += "WHERE pais_destino='{}' ".format(pais_destino)

        if ciudad:
            query += "AND ciudad='{}' ".format(ciudad)
        elif num_agente:
            query += "AND num_agente={} ".format(num_agente)

        query += "AND divisa='{}' ".format(divisa)

        if competidor:
            query += "AND competidor='{}' ".format(competidor)

        if importe_destino:
            if importe_nominal:
                query += "AND importe_nominal={} ".format(importe_nominal)
            query += "AND importe_destino={} ".format(importe_destino)

        if importe_nominal and (not search or search=='pe'): # Second step, when an exact importe nominal does not exist.
            query += "AND importe_nominal={} ".format(importe_nominal)

        if timestamp:
            max_ts = timestamp[0]
            min_ts = timestamp[1]
            query += "AND timestamp < {} AND timestamp > {} ".format(max_ts, min_ts)
        elif geohash:
            max_geohash = geohash[0]
            min_geohash = geohash[1]
            query += "AND geohash < '{}' AND geohash > '{}' ".format(max_geohash, min_geohash)

        query += "LIMIT {}".format(mostrar)
        results = self.session.execute(query)

        rows = []
        for res in results:
            row = []
            for r in res:
                row.append(str(r))
            rows.append(row)

        if not rows or rows[0][0]=='None':
            rows = []

        if timestamp and not importe_destino and not importe_nominal:
            rows = self.best_tasa(alt_table, pais_destino, divisa,
                                     ciudad=ciudad,
                                     importe_destino=rows[0][0],
                                     timestamp=timestamp,
                                     mostrar=10)

        elif timestamp and not importe_destino and importe_nominal and rows and not search:
                        #search!='lower' and search!='upper' and search!='pe':
            rows = self.best_tasa(alt_table, pais_destino, divisa,
                                     ciudad=ciudad,
                                     importe_destino=rows[0][0],
                                     importe_nominal=importe_nominal,
                                     timestamp=timestamp,
                                     search='best_tasa',
                                     mostrar=10)
        return rows

