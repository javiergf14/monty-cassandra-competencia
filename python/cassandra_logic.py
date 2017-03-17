from cassandra.cluster import Cluster
from cassandra.protocol import ConfigurationException
from copy import deepcopy
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

        self.attributes = "pais_destino text, " \
                          + "ciudad text, " \
                          + "divisa text, " \
                          + "importe_destino double, " \
                          + "competidor text, " \
                          + "comision double, " \
                          + "tasa_cambio double, " \
                          + "timestamp double, " \
                          + "lat double, " \
                          + "lon double, " \
                          + "num_agente int, " \
                          + "importe_nominal double, " \
                          + "day int, " \
                          + "month int, " \
                          + "year int, "

        self.attributes_scheme = "pais_destino text, " \
                          + "divisa text, " \
                          + "competidor text, " \

        self.tables = {
            "agente_query": Table.agente(self.attributes),
            "agente_competidor_query": Table.agente_competidor(self.attributes),
            "agente_importe_nominal_query": Table.agente_importe_nominal(self.attributes),
            "agente_competidor_importe_nominal_query": Table.agente_competidor_importe_nominal(self.attributes),
            "ciudad_query": Table.ciudad(self.attributes),
            "ciudad_competidor_query": Table.ciudad_competidor(self.attributes),
            "ciudad_importe_nominal_query": Table.ciudad_importe_nominal(self.attributes),
            "ciudad_competidor_importe_nominal_query": Table.ciudad_competidor_importe_nominal(self.attributes),
            "pais_query": Table.pais(self.attributes),
            "pais_competidor_query": Table.pais_competidor(self.attributes),
            "pais_importe_nominal_query": Table.pais_importe_nominal(self.attributes),
            "pais_competidor_importe_nominal_query": Table.pais_competidor_importe_nominal(self.attributes),
            "geohash_scheme": Table.geohash_scheme(self.attributes_scheme),
            "geohash_competidor_scheme": Table.geohash_competidor_scheme(self.attributes_scheme),
            "geohash_query": Table.geohash(self.attributes),
            "geohash_competidor_query": Table.geohash_competidor(self.attributes),
            "geohash_importe_nominal_query": Table.geohash_importe_nominal(self.attributes),
            "geohash_competidor_importe_nominal_query": Table.geohash_competidor_importe_nominal(self.attributes),
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
        # Column names and values for most of the tables.
        column_names = ["pais_destino", "divisa", "competidor", "timestamp", "ciudad", "comision",
                        "tasa_cambio",  "lat", "lon", "num_agente", "importe_nominal", "day", "month", "year"]

        column_values = []
        for i in column_names:
            column_values.append(data[i])

        column_names.append("importe_destino")
        importe_destino = 100 * float(data["tasa_cambio"]) - float(data["comision"])
        column_values.append(importe_destino)

        # Special case for geolocations: pais_destino, divisa and competidor
        column_names_scheme = column_names[0:3]
        column_values_scheme = column_values[0:3]

        # Fill all the tables.
        for table_name in self.tables.keys():
            column_names_to_insert = deepcopy(column_names_scheme) if table_name.endswith('scheme') else column_names
            column_values_to_insert = deepcopy(column_values_scheme) if table_name.endswith('scheme') else column_values

            if table_name.startswith('geo'):
                column_names_to_insert.append('geohash')
                column_values_to_insert.append(Geohash.encode(float(data['lat']), float(data['lon'])))
                self._insert_data(table_name, column_names_to_insert, column_values_to_insert)
                column_names_to_insert.pop(-1)
                column_values_to_insert.pop(-1)
            else:
                self._insert_data(table_name, column_names_to_insert, column_values_to_insert)

    def _insert_data(self, table_name, column_names, column_values):
        """Insert a row in a table

           Notice we do not need to include all the columns names nor columns values due to the flexible schema.
           The column values are integer by default, except if they are included in the string_set.

           Args:
               table_name (str): name of the table.
               column_names (list): name of the columns to insert.
               column_values (list): values of the columns to insert.
        """
        insert_flag = True
        # Do not insert twice the same data in geolocations.
        if table_name.endswith('scheme'):
            check_data_query = "SELECT geohash FROM {} WHERE ".format(table_name)
            for cont, name in enumerate(column_names):
                value = "'"+column_values[cont]+"'" if name in self.string_set else str(column_values[cont])
                if cont == len(column_names)-1:
                    check_data_query += name+'='+value
                else:
                    check_data_query += name + '=' + value + " AND "

            insert_flag = not self.session.execute(check_data_query).current_rows

        if insert_flag:
            insert_data_query = "INSERT INTO {} (".format(table_name)
            insert_data_query += ",".join(column_names)
            insert_data_query += ") VALUES("
            processed_values = ["'"+c+"'" if column_names[cont] in self.string_set else str(c) for cont, c in enumerate(column_values)]
            insert_data_query += ",".join(processed_values)
            insert_data_query += ")"
            insert_data_query += ";"

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
                  geohash=None,
                  geohash_range = None,
                  timestamp = None,
                  competidor=None,
                  importe_destino=None,
                  importe_nominal=None,
                  range_importe_nominal=None,
                  year=None,
                  month=None,
                  day=None,
                  search=None,
                  alt_table=None,
                  mostrar=None):

        # If query data is between two timestamps, we need a trick to retrieve the row.
        if (timestamp) and not importe_destino and not search:
            sel = 'max(importe_destino)'
        elif search == 'lower':
            sel = 'min(nearest_lower_importe({}, importe_nominal))'.format(importe_nominal)
        elif search == 'upper':
            sel = 'min(nearest_upper_importe({}, importe_nominal))'.format(importe_nominal)
        else:
            sel = '*'

        query = "SELECT {} FROM {} ".format(sel, table_name)
        query += "WHERE pais_destino='{}' ".format(pais_destino)

        # We search by a) ciudad b) num_agente c) proximity coordinates (later).
        if ciudad:
            query += "AND ciudad='{}' ".format(ciudad)
        elif num_agente:
            query += "AND num_agente={} ".format(num_agente)
        elif geohash:
            query += "AND geohash='{}' ".format(geohash)

        query += "AND divisa='{}' ".format(divisa)

        # If concrete competidor specified:
        if competidor:
            query += "AND competidor='{}' ".format(competidor)

        if year and month and day:
            query += "AND year={} AND month={} AND day={} ".format(year, month, day)

        # If max importe destino (used in range of timestamps).
        if importe_destino:
            if importe_nominal:
                query += "AND importe_nominal={} ".format(importe_nominal)
            query += "AND importe_destino={} ".format(importe_destino)

        if importe_nominal and (not search or search == 'approx'):  # Second step, when an exact importe nominal does not exist.
            query += "AND importe_nominal={} ".format(importe_nominal)

        if range_importe_nominal:
            min_importe = range_importe_nominal[0]
            max_importe = range_importe_nominal[1]
            query += "AND importe_nominal <= {} AND importe_nominal >= {} ".format(max_importe, min_importe)

        if timestamp:
            max_ts = timestamp[0]
            min_ts = timestamp[1]
            query += "AND timestamp < {} AND timestamp > {} ".format(max_ts, min_ts)

        elif geohash_range:
            max_geohash = geohash_range[0]
            min_geohash = geohash_range[1]
            query += "AND geohash <= '{}' AND geohash >= '{}' ".format(max_geohash, min_geohash)

        if mostrar and mostrar != -1:
            query += "LIMIT {}".format(mostrar)
        results = self.session.execute(query)

        rows = []
        for res in results:
            row = []
            for r in res:
                row.append(str(r))
            rows.append(row)

        if not rows or rows[0][0] == 'None':
            rows = []

        if timestamp and not importe_destino and not importe_nominal:
            try:
                rows = self.best_tasa(alt_table, pais_destino, divisa,
                                      ciudad=ciudad,
                                      num_agente=num_agente,
                                      geohash=geohash,
                                      competidor=competidor,
                                      importe_destino=rows[0][0],
                                      timestamp=timestamp,
                                      mostrar=10)
            except IndexError:
                rows = ["No existe correspondencia con parametros filtrados"]

        elif timestamp and not importe_destino and importe_nominal and rows and not search:
            rows = self.best_tasa(alt_table, pais_destino, divisa,
                                  ciudad=ciudad,
                                  num_agente=num_agente,
                                  geohash=geohash,
                                  competidor=competidor,
                                  importe_destino=rows[0][0],
                                  importe_nominal=importe_nominal,
                                  timestamp=timestamp,
                                  search='best_tasa',
                                  mostrar=10)


        return rows
