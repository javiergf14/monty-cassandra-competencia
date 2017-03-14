class Table:

    @staticmethod
    def table_ciudad_scheme():
        table_name = "ciudad_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                             + "pais_destino text, " \
                             + "ciudad text, " \
                             + "divisa text, " \
                             + "importe_destino double, " \
                             + "competidor text, " \
                             + "comision double, " \
                             + "tasa_cambio double, " \
                             + "timestamp double, " \
                             + "lat double, " \
                             + "lon double, " \
                             + "num_agente int," \
                             + "importe_nominal double," \
                             + "PRIMARY KEY (pais_destino, ciudad, divisa, importe_destino, competidor) )" \
                               "WITH CLUSTERING ORDER BY (ciudad DESC, divisa DESC, importe_destino DESC, competidor DESC);"
        return table_query

    @staticmethod
    def table_ciudad_competidor_scheme():
        table_name = "ciudad_competidor_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                             + "pais_destino text, " \
                             + "ciudad text, " \
                             + "divisa text, " \
                             + "competidor text, " \
                             + "importe_destino double, " \
                             + "comision double, " \
                             + "tasa_cambio double, " \
                             + "timestamp double, " \
                             + "lat double, " \
                             + "lon double, " \
                             + "num_agente int," \
                             + "importe_nominal double," \
                             + "PRIMARY KEY (pais_destino, ciudad, divisa, competidor, importe_destino))" \
                               "WITH CLUSTERING ORDER BY (ciudad DESC, divisa DESC, competidor DESC, importe_destino DESC);"
        return table_query

    @staticmethod
    def table_ciudad_timestamp_importe_scheme():
        table_name = "ciudad_timestamp_importe_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + "pais_destino text, " \
                      + "ciudad text, " \
                      + "divisa text, " \
                      + "timestamp double, " \
                      + "importe_destino double, " \
                      + "competidor text, " \
                      + "comision double, " \
                      + "tasa_cambio double, " \
                      + "lat double, " \
                      + "lon double, " \
                      + "num_agente int," \
                      + "importe_nominal double," \
                      + "PRIMARY KEY (pais_destino, ciudad, divisa, timestamp, importe_destino, competidor) )" \
                        "WITH CLUSTERING ORDER BY (ciudad DESC, divisa DESC, timestamp DESC, importe_destino DESC, competidor DESC);"
        return table_query

    @staticmethod
    def table_ciudad_importe_timestamp_scheme():
        table_name = "ciudad_importe_timestamp_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + "pais_destino text, " \
                      + "ciudad text, " \
                      + "divisa text, " \
                      + "importe_destino double, " \
                      + "timestamp double, " \
                      + "competidor text, " \
                      + "comision double, " \
                      + "tasa_cambio double, " \
                      + "lat double, " \
                      + "lon double, " \
                      + "num_agente int," \
                      + "importe_nominal double," \
                      + "PRIMARY KEY (pais_destino, ciudad, divisa, importe_destino, timestamp, competidor) )" \
                        "WITH CLUSTERING ORDER BY (ciudad DESC, divisa DESC, importe_destino DESC, timestamp DESC, competidor DESC);"
        return table_query

    @staticmethod
    def table_ciudad_competidor_timestamp_scheme():
        table_name = "ciudad_competidor_timestamp_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + "pais_destino text, " \
                      + "ciudad text, " \
                      + "divisa text, " \
                      + "competidor text, " \
                      + "timestamp double, " \
                      + "importe_destino double, " \
                      + "comision double, " \
                      + "tasa_cambio double, " \
                      + "lat double, " \
                      + "lon double, " \
                      + "num_agente int," \
                      + "importe_nominal double," \
                      + "PRIMARY KEY (pais_destino, ciudad, divisa, competidor, timestamp, importe_destino) )" \
                        "WITH CLUSTERING ORDER BY (ciudad DESC, divisa DESC, competidor DESC, timestamp DESC, importe_destino DESC);"
        return table_query

    @staticmethod
    def table_ciudad_competidor_importe_timestamp_scheme():
        table_name = "ciudad_competidor_importe_timestamp_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + "pais_destino text, " \
                      + "ciudad text, " \
                      + "divisa text, " \
                      + "competidor text, " \
                      + "importe_destino double, " \
                      + "timestamp double, " \
                      + "comision double, " \
                      + "tasa_cambio double, " \
                      + "lat double, " \
                      + "lon double, " \
                      + "num_agente int," \
                      + "importe_nominal double," \
                      + "PRIMARY KEY (pais_destino, ciudad, divisa, competidor, importe_destino, timestamp) )" \
                        "WITH CLUSTERING ORDER BY (ciudad DESC, divisa DESC, competidor DESC, importe_destino DESC, timestamp DESC);"
        return table_query

    @staticmethod
    def table_ciudad_importe_nominal_timestamp_scheme():
        table_name = "ciudad_importe_nominal_timestamp_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + "pais_destino text, " \
                      + "ciudad text, " \
                      + "divisa text, " \
                      + "importe_nominal double," \
                      + "timestamp double, " \
                      + "competidor text, " \
                      + "importe_destino double, " \
                      + "comision double, " \
                      + "tasa_cambio double, " \
                      + "lat double, " \
                      + "lon double, " \
                      + "num_agente int," \
                      + "PRIMARY KEY (pais_destino, ciudad, divisa, importe_nominal, timestamp, competidor) )" \
                        "WITH CLUSTERING ORDER BY (ciudad DESC, divisa DESC, importe_nominal DESC, timestamp DESC, competidor DESC);"
        return table_query

    @staticmethod
    def table_ciudad_importe_nominal_y_destino_timestamp_scheme():
        table_name = "ciudad_importe_nominal_y_destino_timestamp_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + "pais_destino text, " \
                      + "ciudad text, " \
                      + "divisa text, " \
                      + "importe_nominal double," \
                      + "importe_destino double, " \
                      + "timestamp double, " \
                      + "competidor text, " \
                      + "comision double, " \
                      + "tasa_cambio double, " \
                      + "lat double, " \
                      + "lon double, " \
                      + "num_agente int," \
                      + "PRIMARY KEY (pais_destino, ciudad, divisa, importe_nominal, importe_destino, timestamp, competidor) )" \
                        "WITH CLUSTERING ORDER BY (ciudad DESC, divisa DESC, importe_nominal DESC, importe_destino DESC, timestamp DESC, competidor DESC);"
        return table_query




    @staticmethod
    def table_ciudad_timestamp_importe_nominal_scheme():
        table_name = "ciudad_timestamp_importe_nominal_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + "pais_destino text, " \
                      + "ciudad text, " \
                      + "divisa text, " \
                      + "timestamp double, " \
                      + "importe_nominal double," \
                      + "competidor text, " \
                      + "importe_destino double, " \
                      + "comision double, " \
                      + "tasa_cambio double, " \
                      + "lat double, " \
                      + "lon double, " \
                      + "num_agente int," \
                      + "PRIMARY KEY (pais_destino, ciudad, divisa, timestamp, importe_nominal, competidor) )" \
                        "WITH CLUSTERING ORDER BY (ciudad DESC, divisa DESC, timestamp DESC, importe_nominal DESC, competidor DESC);"
        return table_query

    @staticmethod
    def table_geohash_scheme():
        table_name = "geohash_query"
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
                             + "num_agente int, " \
                             + "ciudad text, " \
                             + "importe_nominal double," \
                             + "PRIMARY KEY (pais_destino, divisa, geohash, importe_destino, competidor))" \
                               "WITH CLUSTERING ORDER BY (divisa DESC, geohash DESC, importe_destino DESC, competidor DESC);"

        return table_query

    @staticmethod
    def table_geohash_competidor_scheme():
        table_name = "geohash_competidor_query"
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
                             + "num_agente int, " \
                             + "ciudad text, " \
                             + "importe_nominal double," \
                             + "PRIMARY KEY (pais_destino, divisa, competidor, geohash, importe_destino))" \
                               "WITH CLUSTERING ORDER BY (divisa DESC, competidor DESC, geohash DESC, importe_destino DESC);"

        return table_query

    @staticmethod
    def table_agente_scheme():
        table_name = "agente_query"
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
                      + "lat double, " \
                      + "lon double, " \
                      + "importe_nominal double," \
                      + "PRIMARY KEY (pais_destino, num_agente, divisa, importe_destino, competidor) )" \
                        "WITH CLUSTERING ORDER BY (num_agente DESC, divisa DESC, importe_destino DESC, competidor DESC);"
        return table_query

    @staticmethod
    def table_agente_competidor_scheme():
        table_name = "agente_competidor_query"
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
                      + "lat double, " \
                      + "lon double, " \
                      + "importe_nominal double," \
                      + "PRIMARY KEY (pais_destino, num_agente, divisa, competidor, importe_destino) )" \
                        "WITH CLUSTERING ORDER BY (num_agente DESC, divisa DESC, competidor DESC, importe_destino DESC);"
        return table_query



