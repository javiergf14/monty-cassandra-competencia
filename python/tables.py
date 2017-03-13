

class Table:

    @staticmethod
    def table1_scheme():
        table_name = "query1"
        table_query = "CREATE TABLE " + table_name + "(" \
                             + "pais_destino text, " \
                             + "ciudad text, " \
                             + "divisa text, " \
                             + "importe_destino double, " \
                             + "competidor text, " \
                             + "comision double, " \
                             + "tasa_cambio double, " \
                             + "timestamp double, " \
                             + "PRIMARY KEY (pais_destino, ciudad, divisa, importe_destino, competidor) )" \
                               "WITH CLUSTERING ORDER BY (ciudad DESC, divisa DESC, importe_destino DESC, competidor DESC);"
        return table_query

    @staticmethod
    def table2_scheme():
        table_name = "query2"
        table_query = "CREATE TABLE " + table_name + "(" \
                             + "pais_destino text, " \
                             + "ciudad text, " \
                             + "divisa text, " \
                             + "competidor text, " \
                             + "importe_destino double, " \
                             + "comision double, " \
                             + "tasa_cambio double, " \
                             + "timestamp double, " \
                             + "PRIMARY KEY (pais_destino, ciudad, divisa, competidor, importe_destino))" \
                               "WITH CLUSTERING ORDER BY (ciudad DESC, divisa DESC, competidor DESC, importe_destino DESC);"
        return table_query

    @staticmethod
    def table3_scheme():
        table_name = "query3"
        table_query = "CREATE TABLE " + table_name + "(" \
                             + "pais_destino text, " \
                             + "divisa text, " \
                             + "geohash text, " \
                             + "importe_destino double, " \
                             + "competidor text, " \
                             + "comision double, " \
                             + "tasa_cambio double, " \
                             + "timestamp double, " \
                             + "lat double, " \
                             + "lon double, " \
                             + "ciudad text, " \
                             + "PRIMARY KEY (pais_destino, divisa, geohash, importe_destino, competidor))" \
                               "WITH CLUSTERING ORDER BY (divisa DESC, geohash DESC, importe_destino DESC, competidor DESC);"

        return table_query

    @staticmethod
    def table4_scheme():
        table_name = "query4"
        table_query = "CREATE TABLE " + table_name + "(" \
                             + "pais_destino text, " \
                             + "divisa text, " \
                             + "competidor text, " \
                             + "geohash text, " \
                             + "importe_destino double, " \
                             + "comision double, " \
                             + "tasa_cambio double, " \
                             + "timestamp double, " \
                             + "lat double, " \
                             + "lon double, " \
                             + "ciudad text, " \
                             + "PRIMARY KEY (pais_destino, divisa, competidor, geohash, importe_destino))" \
                               "WITH CLUSTERING ORDER BY (divisa DESC, competidor DESC, geohash DESC, importe_destino DESC);"

        return table_query

    @staticmethod
    def table5_scheme():
        table_name = "query5"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + "pais_destino text, " \
                      + "num_agente int, " \
                      + "divisa text, " \
                      + "importe_destino double, " \
                      + "competidor text, " \
                      + "comision double, " \
                      + "tasa_cambio double, " \
                      + "timestamp double, " \
                      + "ciudad text, " \
                      + "PRIMARY KEY (pais_destino, num_agente, divisa, importe_destino, competidor) )" \
                        "WITH CLUSTERING ORDER BY (num_agente DESC, divisa DESC, importe_destino DESC, competidor DESC);"
        return table_query

    @staticmethod
    def table6_scheme():
        table_name = "query6"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + "pais_destino text, " \
                      + "num_agente int, " \
                      + "divisa text, " \
                      + "competidor text, " \
                      + "importe_destino double, " \
                      + "comision double, " \
                      + "tasa_cambio double, " \
                      + "timestamp double, " \
                      + "ciudad text, " \
                      + "PRIMARY KEY (pais_destino, num_agente, divisa, competidor, importe_destino) )" \
                        "WITH CLUSTERING ORDER BY (num_agente DESC, divisa DESC, competidor DESC, importe_destino DESC);"
        return table_query
