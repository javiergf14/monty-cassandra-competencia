class Table:

    # QUERIES SETTING agente.

    # QUERY WITHOUT RESTRICTIONS.
    # For all competitors.
    @staticmethod
    def ciudad(attributes):
        table_name = "ciudad_query"
        table_query = "CREATE TABLE " + table_name + "("  \
                      + attributes \
                      + "PRIMARY KEY (pais_destino, ciudad, divisa, importe_destino, competidor) )" \
                        "WITH CLUSTERING ORDER BY (ciudad DESC, divisa DESC, importe_destino DESC, competidor DESC);"
        return table_query

    # For one competitor.
    @staticmethod
    def ciudad_competidor(attributes):
        table_name = "ciudad_competidor_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                             + attributes \
                             + "PRIMARY KEY (pais_destino, ciudad, divisa, competidor, importe_destino))" \
                               "WITH CLUSTERING ORDER BY (ciudad DESC, divisa DESC, competidor DESC, importe_destino DESC);"
        return table_query

    # QUERY WITH TIMESTAMP RANGE.
    @staticmethod
    # For all competitors.
    def ciudad_ts_importe(attributes):
        table_name = "ciudad_ts_importe_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "PRIMARY KEY (pais_destino, ciudad, divisa, timestamp, importe_destino, competidor) )" \
                        "WITH CLUSTERING ORDER BY (ciudad DESC, divisa DESC, timestamp DESC, importe_destino DESC, competidor DESC);"
        return table_query

    @staticmethod
    def ciudad_importe_ts(attributes):
        table_name = "ciudad_importe_ts_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "PRIMARY KEY (pais_destino, ciudad, divisa, importe_destino, timestamp, competidor) )" \
                        "WITH CLUSTERING ORDER BY (ciudad DESC, divisa DESC, importe_destino DESC, timestamp DESC, competidor DESC);"
        return table_query

    # For one competitor.
    @staticmethod
    def ciudad_competidor_ts_importe(attributes):
        table_name = "ciudad_competidor_ts_importe_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "PRIMARY KEY (pais_destino, ciudad, divisa, competidor, timestamp, importe_destino) )" \
                        "WITH CLUSTERING ORDER BY (ciudad DESC, divisa DESC, competidor DESC, timestamp DESC, importe_destino DESC);"
        return table_query

    @staticmethod
    def ciudad_competidor_importe_ts(attributes):
        table_name = "ciudad_competidor_importe_ts_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "PRIMARY KEY (pais_destino, ciudad, divisa, competidor, importe_destino, timestamp) )" \
                        "WITH CLUSTERING ORDER BY (ciudad DESC, divisa DESC, competidor DESC, importe_destino DESC, timestamp DESC);"
        return table_query


    # QUERY WITH EXACT IMPORTE.
    @staticmethod
    # For all competitors.
    def ciudad_importe_nominal_ts(attributes):
        table_name = "ciudad_importe_nominal_ts_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "PRIMARY KEY (pais_destino, ciudad, divisa, importe_nominal, timestamp, competidor) )" \
                        "WITH CLUSTERING ORDER BY (ciudad DESC, divisa DESC, importe_nominal DESC, timestamp DESC, competidor DESC);"
        return table_query

    @staticmethod
    def ciudad_importes_ts(attributes):
        table_name = "ciudad_importes_ts_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "PRIMARY KEY (pais_destino, ciudad, divisa, importe_nominal, importe_destino, timestamp, competidor) )" \
                        "WITH CLUSTERING ORDER BY (ciudad DESC, divisa DESC, importe_nominal DESC, importe_destino DESC, timestamp DESC, competidor DESC);"
        return table_query


    @staticmethod
    def ciudad_ts_importe_nominal(attributes):
        table_name = "ciudad_ts_importe_nominal_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "PRIMARY KEY (pais_destino, ciudad, divisa, timestamp, importe_nominal, competidor) )" \
                        "WITH CLUSTERING ORDER BY (ciudad DESC, divisa DESC, timestamp DESC, importe_nominal DESC, competidor DESC);"
        return table_query

    # For one competitor.
    @staticmethod
    def ciudad_competidor_importe_nominal_ts(attributes):
        table_name = "ciudad_competidor_importe_nominal_ts_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "PRIMARY KEY (pais_destino, ciudad, divisa, competidor, importe_nominal, timestamp) )" \
                        "WITH CLUSTERING ORDER BY (ciudad DESC, divisa DESC, competidor DESC, importe_nominal DESC, timestamp DESC);"
        return table_query

    @staticmethod
    def ciudad_competidor_importes_ts(attributes):
        table_name = "ciudad_competidor_importes_ts_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "PRIMARY KEY (pais_destino, ciudad, divisa, competidor, importe_nominal, importe_destino, timestamp) )" \
                        "WITH CLUSTERING ORDER BY (ciudad DESC, divisa DESC, competidor DESC, importe_nominal DESC, importe_destino DESC, timestamp DESC);"
        return table_query


    @staticmethod
    def ciudad_competidor_ts_importe_nominal(attributes):
        table_name = "ciudad_competidor_ts_importe_nominal_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "PRIMARY KEY (pais_destino, ciudad, divisa, competidor, timestamp, importe_nominal) )" \
                        "WITH CLUSTERING ORDER BY (ciudad DESC, divisa DESC, competidor DESC, timestamp DESC, importe_nominal DESC);"
        return table_query


    # QUERY WITH RANGE IMPORTE.
    # For all competitors.
    @staticmethod
    def ciudad_fecha_importe_nominal(attributes):
        table_name = "ciudad_fecha_importe_nominal_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "PRIMARY KEY (pais_destino, ciudad, divisa, year, month, day, importe_nominal, importe_destino, timestamp) )" \
                        "WITH CLUSTERING ORDER BY (ciudad DESC, divisa DESC, year DESC, month DESC, day DESC, importe_nominal DESC, importe_destino DESC, timestamp DESC);"
        return table_query

    @staticmethod
    def ciudad_fecha_importe_destino(attributes):
        table_name = "ciudad_fecha_importe_destino_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "PRIMARY KEY (pais_destino, ciudad, divisa, year, month, day, importe_destino, importe_nominal, timestamp) )" \
                        "WITH CLUSTERING ORDER BY (ciudad DESC, divisa DESC, year DESC, month DESC, day DESC, importe_destino DESC, importe_nominal DESC, timestamp DESC);"
        return table_query

    # For one competitor.
    @staticmethod
    def ciudad_competidor_fecha_importe_nominal(attributes):
        table_name = "ciudad_competidor_fecha_importe_nominal_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "PRIMARY KEY (pais_destino, ciudad, divisa, competidor, year, month, day, importe_nominal, importe_destino, timestamp) )" \
                        "WITH CLUSTERING ORDER BY (ciudad DESC, divisa DESC, competidor DESC, year DESC, month DESC, day DESC, importe_nominal DESC, importe_destino DESC, timestamp DESC);"
        return table_query

    @staticmethod
    def ciudad_competidor_fecha_importe_destino(attributes):
        table_name = "ciudad_competidor_fecha_importe_destino_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "PRIMARY KEY (pais_destino, ciudad, divisa, competidor, year, month, day, importe_destino, importe_nominal, timestamp) )" \
                        "WITH CLUSTERING ORDER BY (ciudad DESC, divisa DESC, competidor DESC, year DESC, month DESC, day DESC, importe_destino DESC, importe_nominal DESC, timestamp DESC);"
        return table_query

    # QUERIES SETTING AGENTE.
    # QUERY WITHOUT RESTRICTIONS.
    # For all competitors.
    @staticmethod
    def agente(attributes):
        table_name = "agente_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "PRIMARY KEY (pais_destino, num_agente, divisa, importe_destino, competidor) )" \
                        "WITH CLUSTERING ORDER BY (num_agente DESC, divisa DESC, importe_destino DESC, competidor DESC);"
        return table_query

    # For one competitor.
    @staticmethod
    def agente_competidor(attributes):
        table_name = "agente_competidor_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "PRIMARY KEY (pais_destino, num_agente, divisa, competidor, importe_destino) )" \
                        "WITH CLUSTERING ORDER BY (num_agente DESC, divisa DESC, competidor DESC, importe_destino DESC);"
        return table_query

    # QUERY WITH TIMESTAMP RANGE.
    @staticmethod
    # For all competitors.
    def agente_ts_importe(attributes):
        table_name = "agente_ts_importe_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "PRIMARY KEY (pais_destino, num_agente, divisa, timestamp, importe_destino, competidor) )" \
                        "WITH CLUSTERING ORDER BY (num_agente DESC, divisa DESC, timestamp DESC, importe_destino DESC, competidor DESC);"
        return table_query

    @staticmethod
    def agente_importe_ts(attributes):
        table_name = "agente_importe_ts_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "PRIMARY KEY (pais_destino, num_agente, divisa, importe_destino, timestamp, competidor) )" \
                        "WITH CLUSTERING ORDER BY (num_agente DESC, divisa DESC, importe_destino DESC, timestamp DESC, competidor DESC);"
        return table_query

    # For one competitor.
    @staticmethod
    def agente_competidor_ts_importe(attributes):
        table_name = "agente_competidor_ts_importe_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "PRIMARY KEY (pais_destino, num_agente, divisa, competidor, timestamp, importe_destino) )" \
                        "WITH CLUSTERING ORDER BY (num_agente DESC, divisa DESC, competidor DESC, timestamp DESC, importe_destino DESC);"
        return table_query

    @staticmethod
    def agente_competidor_importe_ts(attributes):
        table_name = "agente_competidor_importe_ts_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "PRIMARY KEY (pais_destino, num_agente, divisa, competidor, importe_destino, timestamp) )" \
                        "WITH CLUSTERING ORDER BY (num_agente DESC, divisa DESC, competidor DESC, importe_destino DESC, timestamp DESC);"
        return table_query

    # QUERY WITH EXACT IMPORTE.
    @staticmethod
    # For all competitors.
    def agente_importe_nominal_ts(attributes):
        table_name = "agente_importe_nominal_ts_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "PRIMARY KEY (pais_destino, num_agente, divisa, importe_nominal, timestamp, competidor) )" \
                        "WITH CLUSTERING ORDER BY (num_agente DESC, divisa DESC, importe_nominal DESC, timestamp DESC, competidor DESC);"
        return table_query

    @staticmethod
    def agente_importes_ts(attributes):
        table_name = "agente_importes_ts_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "PRIMARY KEY (pais_destino, num_agente, divisa, importe_nominal, importe_destino, timestamp, competidor) )" \
                        "WITH CLUSTERING ORDER BY (num_agente DESC, divisa DESC, importe_nominal DESC, importe_destino DESC, timestamp DESC, competidor DESC);"
        return table_query


    @staticmethod
    def agente_ts_importe_nominal(attributes):
        table_name = "agente_ts_importe_nominal_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "PRIMARY KEY (pais_destino, num_agente, divisa, timestamp, importe_nominal, competidor) )" \
                        "WITH CLUSTERING ORDER BY (num_agente DESC, divisa DESC, timestamp DESC, importe_nominal DESC, competidor DESC);"
        return table_query

    # For one competitor.
    @staticmethod
    def agente_competidor_importe_nominal_ts(attributes):
        table_name = "agente_competidor_importe_nominal_ts_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "PRIMARY KEY (pais_destino, num_agente, divisa, competidor, importe_nominal, timestamp) )" \
                        "WITH CLUSTERING ORDER BY (num_agente DESC, divisa DESC, competidor DESC, importe_nominal DESC, timestamp DESC);"
        return table_query

    @staticmethod
    def agente_competidor_importes_ts(attributes):
        table_name = "agente_competidor_importes_ts_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "PRIMARY KEY (pais_destino, num_agente, divisa, competidor, importe_nominal, importe_destino, timestamp) )" \
                        "WITH CLUSTERING ORDER BY (num_agente DESC, divisa DESC, competidor DESC, importe_nominal DESC, importe_destino DESC, timestamp DESC);"
        return table_query


    @staticmethod
    def agente_competidor_ts_importe_nominal(attributes):
        table_name = "agente_competidor_ts_importe_nominal_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "PRIMARY KEY (pais_destino, num_agente, divisa, competidor, timestamp, importe_nominal) )" \
                        "WITH CLUSTERING ORDER BY (num_agente DESC, divisa DESC, competidor DESC, timestamp DESC, importe_nominal DESC);"
        return table_query

    # QUERY WITH RANGE IMPORTE.
    # For all competitors.
    @staticmethod
    def agente_fecha_importe_nominal(attributes):
        table_name = "agente_fecha_importe_nominal_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "PRIMARY KEY (pais_destino, num_agente, divisa, year, month, day, importe_nominal, importe_destino, timestamp) )" \
                        "WITH CLUSTERING ORDER BY (num_agente DESC, divisa DESC, year DESC, month DESC, day DESC, importe_nominal DESC, importe_destino DESC, timestamp DESC);"
        return table_query

    @staticmethod
    def agente_fecha_importe_destino(attributes):
        table_name = "agente_fecha_importe_destino_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "PRIMARY KEY (pais_destino, num_agente, divisa, year, month, day, importe_destino, importe_nominal, timestamp) )" \
                        "WITH CLUSTERING ORDER BY (num_agente DESC, divisa DESC, year DESC, month DESC, day DESC, importe_destino DESC, importe_nominal DESC, timestamp DESC);"
        return table_query

    # For one competitor.
    @staticmethod
    def agente_competidor_fecha_importe_nominal(attributes):
        table_name = "agente_competidor_fecha_importe_nominal_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "PRIMARY KEY (pais_destino, num_agente, divisa, competidor, year, month, day, importe_nominal, importe_destino, timestamp) )" \
                        "WITH CLUSTERING ORDER BY (num_agente DESC, divisa DESC, competidor DESC, year DESC, month DESC, day DESC, importe_nominal DESC, importe_destino DESC, timestamp DESC);"
        return table_query

    @staticmethod
    def agente_competidor_fecha_importe_destino(attributes):
        table_name = "agente_competidor_fecha_importe_destino_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "PRIMARY KEY (pais_destino, num_agente, divisa, competidor, year, month, day, importe_destino, importe_nominal, timestamp) )" \
                        "WITH CLUSTERING ORDER BY (num_agente DESC, divisa DESC, competidor DESC, year DESC, month DESC, day DESC, importe_destino DESC, importe_nominal DESC, timestamp DESC);"
        return table_query



    # QUERIES SETTING GEOPOSITION.

    # Tables for storing unique geopositions.
    @staticmethod
    def geohash_scheme(attributes):
        table_name = "geohash_scheme"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "geohash text, " \
                      + "PRIMARY KEY (pais_destino, divisa, geohash, competidor))" \
                        "WITH CLUSTERING ORDER BY (divisa DESC, geohash DESC, competidor DESC);"

        return table_query

    @staticmethod
    def geohash_competidor_scheme(attributes):
        table_name = "geohash_competidor_scheme"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "geohash text, " \
                      + "PRIMARY KEY (pais_destino, divisa, competidor, geohash))" \
                        "WITH CLUSTERING ORDER BY (divisa DESC, competidor DESC, geohash DESC);"

        return table_query

    # QUERY WITHOUT RESTRICTIONS.
    # For all competitors.
    @staticmethod
    def geohash(attributes):
        table_name = "geohash_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "geohash text, " \
                      + "PRIMARY KEY (pais_destino, geohash, divisa, importe_destino, competidor) )" \
                        "WITH CLUSTERING ORDER BY (geohash DESC, divisa DESC, importe_destino DESC, competidor DESC);"
        return table_query

    # For one competitor.
    @staticmethod
    def geohash_competidor(attributes):
        table_name = "geohash_competidor_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "geohash text, " \
                      + "PRIMARY KEY (pais_destino, geohash, divisa, competidor, importe_destino))" \
                        "WITH CLUSTERING ORDER BY (geohash DESC, divisa DESC, competidor DESC, importe_destino DESC);"
        return table_query

    # QUERY WITH TIMESTAMP RANGE.
    @staticmethod
    # For all competitors.
    def geohash_ts_importe(attributes):
        table_name = "geohash_ts_importe_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "geohash text, " \
                      + "PRIMARY KEY (pais_destino, geohash, divisa, timestamp, importe_destino, competidor) )" \
                        "WITH CLUSTERING ORDER BY (geohash DESC, divisa DESC, timestamp DESC, importe_destino DESC, competidor DESC);"
        return table_query

    @staticmethod
    def geohash_importe_ts(attributes):
        table_name = "geohash_importe_ts_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "geohash text, " \
                      + "PRIMARY KEY (pais_destino, geohash, divisa, importe_destino, timestamp, competidor) )" \
                        "WITH CLUSTERING ORDER BY (geohash DESC, divisa DESC, importe_destino DESC, timestamp DESC, competidor DESC);"
        return table_query

    # For one competitor.
    @staticmethod
    def geohash_competidor_ts_importe(attributes):
        table_name = "geohash_competidor_ts_importe_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "geohash text, " \
                      + "PRIMARY KEY (pais_destino, geohash, divisa, competidor, timestamp, importe_destino) )" \
                        "WITH CLUSTERING ORDER BY (geohash DESC, divisa DESC, competidor DESC, timestamp DESC, importe_destino DESC);"
        return table_query

    @staticmethod
    def geohash_competidor_importe_ts(attributes):
        table_name = "geohash_competidor_importe_ts_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "geohash text, " \
                      + "PRIMARY KEY (pais_destino, geohash, divisa, competidor, importe_destino, timestamp) )" \
                        "WITH CLUSTERING ORDER BY (geohash DESC, divisa DESC, competidor DESC, importe_destino DESC, timestamp DESC);"
        return table_query

    # QUERY WITH EXACT IMPORTE.
    @staticmethod
    # For all competitors.
    def geohash_importe_nominal_ts(attributes):
        table_name = "geohash_importe_nominal_ts_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "geohash text, " \
                      + "PRIMARY KEY (pais_destino, geohash, divisa, importe_nominal, timestamp, competidor) )" \
                        "WITH CLUSTERING ORDER BY (geohash DESC, divisa DESC, importe_nominal DESC, timestamp DESC, competidor DESC);"
        return table_query

    @staticmethod
    def geohash_importes_ts(attributes):
        table_name = "geohash_importes_ts_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "geohash text, " \
                      + "PRIMARY KEY (pais_destino, geohash, divisa, importe_nominal, importe_destino, timestamp, competidor) )" \
                        "WITH CLUSTERING ORDER BY (geohash DESC, divisa DESC, importe_nominal DESC, importe_destino DESC, timestamp DESC, competidor DESC);"
        return table_query

    @staticmethod
    def geohash_ts_importe_nominal(attributes):
        table_name = "geohash_ts_importe_nominal_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "geohash text, " \
                      + "PRIMARY KEY (pais_destino, geohash, divisa, timestamp, importe_nominal, competidor) )" \
                        "WITH CLUSTERING ORDER BY (geohash DESC, divisa DESC, timestamp DESC, importe_nominal DESC, competidor DESC);"
        return table_query

    # For one competitor.
    @staticmethod
    def geohash_competidor_importe_nominal_ts(attributes):
        table_name = "geohash_competidor_importe_nominal_ts_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "geohash text, " \
                      + "PRIMARY KEY (pais_destino, geohash, divisa, competidor, importe_nominal, timestamp) )" \
                        "WITH CLUSTERING ORDER BY (geohash DESC, divisa DESC, competidor DESC, importe_nominal DESC, timestamp DESC);"
        return table_query

    @staticmethod
    def geohash_competidor_importes_ts(attributes):
        table_name = "geohash_competidor_importes_ts_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "geohash text, " \
                      + "PRIMARY KEY (pais_destino, geohash, divisa, competidor, importe_nominal, importe_destino, timestamp) )" \
                        "WITH CLUSTERING ORDER BY (geohash DESC, divisa DESC, competidor DESC, importe_nominal DESC, importe_destino DESC, timestamp DESC);"
        return table_query

    @staticmethod
    def geohash_competidor_ts_importe_nominal(attributes):
        table_name = "geohash_competidor_ts_importe_nominal_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "geohash text, " \
                      + "PRIMARY KEY (pais_destino, geohash, divisa, competidor, timestamp, importe_nominal) )" \
                        "WITH CLUSTERING ORDER BY (geohash DESC, divisa DESC, competidor DESC, timestamp DESC, importe_nominal DESC);"
        return table_query

    # QUERY WITH RANGE IMPORTE.
    # For all competitors.
    @staticmethod
    def geohash_fecha_importe_nominal(attributes):
        table_name = "geohash_fecha_importe_nominal_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "geohash text, " \
                      + "PRIMARY KEY (pais_destino, geohash, divisa, year, month, day, importe_nominal, importe_destino, timestamp) )" \
                        "WITH CLUSTERING ORDER BY (geohash DESC, divisa DESC, year DESC, month DESC, day DESC, importe_nominal DESC, importe_destino DESC, timestamp DESC);"
        return table_query

    @staticmethod
    def geohash_fecha_importe_destino(attributes):
        table_name = "geohash_fecha_importe_destino_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "geohash text, " \
                      + "PRIMARY KEY (pais_destino, geohash, divisa, year, month, day, importe_destino, importe_nominal, timestamp) )" \
                        "WITH CLUSTERING ORDER BY (geohash DESC, divisa DESC, year DESC, month DESC, day DESC, importe_destino DESC, importe_nominal DESC, timestamp DESC);"
        return table_query

    # For one competitor.
    @staticmethod
    def geohash_competidor_fecha_importe_nominal(attributes):
        table_name = "geohash_competidor_fecha_importe_nominal_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "geohash text, " \
                      + "PRIMARY KEY (pais_destino, geohash, divisa, competidor, year, month, day, importe_nominal, importe_destino, timestamp) )" \
                        "WITH CLUSTERING ORDER BY (geohash DESC, divisa DESC, competidor DESC, year DESC, month DESC, day DESC, importe_nominal DESC, importe_destino DESC, timestamp DESC);"
        return table_query

    @staticmethod
    def geohash_competidor_fecha_importe_destino(attributes):
        table_name = "geohash_competidor_fecha_importe_destino_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "geohash text, " \
                      + "PRIMARY KEY (pais_destino, geohash, divisa, competidor, year, month, day, importe_destino, importe_nominal, timestamp) )" \
                        "WITH CLUSTERING ORDER BY (geohash DESC, divisa DESC, competidor DESC, year DESC, month DESC, day DESC, importe_destino DESC, importe_nominal DESC, timestamp DESC);"
        return table_query



