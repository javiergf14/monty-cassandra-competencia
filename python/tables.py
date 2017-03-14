class Table:

    @staticmethod
    def table_ciudad_scheme(attributes):


        table_name = "ciudad_query"
        table_query = "CREATE TABLE " + table_name + "("  \
                      + attributes \
                      + "PRIMARY KEY (pais_destino, ciudad, divisa, importe_destino, competidor) )" \
                        "WITH CLUSTERING ORDER BY (ciudad DESC, divisa DESC, importe_destino DESC, competidor DESC);"
        return table_query

    @staticmethod
    def table_ciudad_competidor_scheme(attributes):
        table_name = "ciudad_competidor_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                             + attributes \
                             + "PRIMARY KEY (pais_destino, ciudad, divisa, competidor, importe_destino))" \
                               "WITH CLUSTERING ORDER BY (ciudad DESC, divisa DESC, competidor DESC, importe_destino DESC);"
        return table_query

    @staticmethod
    def table_ciudad_timestamp_importe_scheme(attributes):
        table_name = "ciudad_timestamp_importe_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "PRIMARY KEY (pais_destino, ciudad, divisa, timestamp, importe_destino, competidor) )" \
                        "WITH CLUSTERING ORDER BY (ciudad DESC, divisa DESC, timestamp DESC, importe_destino DESC, competidor DESC);"
        return table_query

    @staticmethod
    def table_ciudad_importe_timestamp_scheme(attributes):
        table_name = "ciudad_importe_timestamp_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "PRIMARY KEY (pais_destino, ciudad, divisa, importe_destino, timestamp, competidor) )" \
                        "WITH CLUSTERING ORDER BY (ciudad DESC, divisa DESC, importe_destino DESC, timestamp DESC, competidor DESC);"
        return table_query

    @staticmethod
    def table_ciudad_competidor_timestamp_scheme(attributes):
        table_name = "ciudad_competidor_timestamp_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "PRIMARY KEY (pais_destino, ciudad, divisa, competidor, timestamp, importe_destino) )" \
                        "WITH CLUSTERING ORDER BY (ciudad DESC, divisa DESC, competidor DESC, timestamp DESC, importe_destino DESC);"
        return table_query

    @staticmethod
    def table_ciudad_competidor_importe_timestamp_scheme(attributes):
        table_name = "ciudad_competidor_importe_timestamp_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "PRIMARY KEY (pais_destino, ciudad, divisa, competidor, importe_destino, timestamp) )" \
                        "WITH CLUSTERING ORDER BY (ciudad DESC, divisa DESC, competidor DESC, importe_destino DESC, timestamp DESC);"
        return table_query

    @staticmethod
    def table_ciudad_importe_nominal_timestamp_scheme(attributes):
        table_name = "ciudad_importe_nominal_timestamp_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "PRIMARY KEY (pais_destino, ciudad, divisa, importe_nominal, timestamp, competidor) )" \
                        "WITH CLUSTERING ORDER BY (ciudad DESC, divisa DESC, importe_nominal DESC, timestamp DESC, competidor DESC);"
        return table_query

    @staticmethod
    def table_ciudad_importe_nominal_y_destino_timestamp_scheme(attributes):
        table_name = "ciudad_importe_nominal_y_destino_timestamp_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "PRIMARY KEY (pais_destino, ciudad, divisa, importe_nominal, importe_destino, timestamp, competidor) )" \
                        "WITH CLUSTERING ORDER BY (ciudad DESC, divisa DESC, importe_nominal DESC, importe_destino DESC, timestamp DESC, competidor DESC);"
        return table_query


    @staticmethod
    def table_ciudad_timestamp_importe_nominal_scheme(attributes):
        table_name = "ciudad_timestamp_importe_nominal_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "PRIMARY KEY (pais_destino, ciudad, divisa, timestamp, importe_nominal, competidor) )" \
                        "WITH CLUSTERING ORDER BY (ciudad DESC, divisa DESC, timestamp DESC, importe_nominal DESC, competidor DESC);"
        return table_query

    @staticmethod
    def table_ciudad_fecha_importe_nominal_scheme(attributes):
        table_name = "ciudad_fecha_importe_nominal_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "PRIMARY KEY (pais_destino, ciudad, divisa, year, month, day, importe_nominal, importe_destino, timestamp) )" \
                        "WITH CLUSTERING ORDER BY (ciudad DESC, divisa DESC, year DESC, month DESC, day DESC, importe_nominal DESC, importe_destino DESC, timestamp DESC);"
        return table_query

    @staticmethod
    def table_ciudad_fecha_importe_destino_scheme(attributes):
        table_name = "ciudad_fecha_importe_destino_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "PRIMARY KEY (pais_destino, ciudad, divisa, year, month, day, importe_destino, importe_nominal, timestamp) )" \
                        "WITH CLUSTERING ORDER BY (ciudad DESC, divisa DESC, year DESC, month DESC, day DESC, importe_destino DESC, importe_nominal DESC, timestamp DESC);"
        return table_query

    @staticmethod
    def table_geohash_scheme(attributes):
        table_name = "geohash_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "geohash text, " \
                      + "PRIMARY KEY (pais_destino, divisa, geohash, importe_destino, competidor))" \
                        "WITH CLUSTERING ORDER BY (divisa DESC, geohash DESC, importe_destino DESC, competidor DESC);"

        return table_query

    @staticmethod
    def table_geohash_competidor_scheme(attributes):
        table_name = "geohash_competidor_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "geohash text, " \
                      + "PRIMARY KEY (pais_destino, divisa, competidor, geohash, importe_destino))" \
                        "WITH CLUSTERING ORDER BY (divisa DESC, competidor DESC, geohash DESC, importe_destino DESC);"

        return table_query

    @staticmethod
    def table_agente_scheme(attributes):
        table_name = "agente_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "PRIMARY KEY (pais_destino, num_agente, divisa, importe_destino, competidor) )" \
                        "WITH CLUSTERING ORDER BY (num_agente DESC, divisa DESC, importe_destino DESC, competidor DESC);"
        return table_query

    @staticmethod
    def table_agente_competidor_scheme(attributes):
        table_name = "agente_competidor_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "PRIMARY KEY (pais_destino, num_agente, divisa, competidor, importe_destino) )" \
                        "WITH CLUSTERING ORDER BY (num_agente DESC, divisa DESC, competidor DESC, importe_destino DESC);"
        return table_query