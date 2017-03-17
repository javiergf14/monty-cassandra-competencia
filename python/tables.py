class Table:
    ########### NUM_AGENTE ##############
    # This solves the query: Dime la mejor tasa sin importar el importe nominal (dia especifico o rango de fechas iterando).
    @staticmethod
    def agente(attributes):
        table_name = "agente_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "PRIMARY KEY (pais_destino, num_agente, divisa, year, month, day, importe_destino, timestamp) )" \
                        "WITH CLUSTERING ORDER BY (num_agente DESC, divisa DESC, year DESC, month DESC, day DESC, importe_destino DESC, timestamp DESC);"
        return table_query

    @staticmethod
    def agente_competidor(attributes):
        table_name = "agente_competidor_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "PRIMARY KEY (pais_destino, num_agente, divisa, competidor, year, month, day, importe_destino, timestamp) )" \
                        "WITH CLUSTERING ORDER BY (num_agente DESC, divisa DESC, competidor DESC, year DESC, month DESC, day DESC, importe_destino DESC, timestamp DESC);"
        return table_query

    # This solves the query: Dime la mejor tasa para un dia especifico y para un importe nominal especifico o rango de importes iterando (dia especifico o rango de fechas iterando).
    @staticmethod
    def agente_importe_nominal(attributes):
        table_name = "agente_importe_nominal_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "PRIMARY KEY (pais_destino, num_agente, divisa, year, month, day, importe_nominal, importe_destino, timestamp) )" \
                        "WITH CLUSTERING ORDER BY (num_agente DESC, divisa DESC, year DESC, month DESC, day DESC, importe_nominal DESC, importe_destino DESC, timestamp DESC);"
        return table_query

    @staticmethod
    def agente_competidor_importe_nominal(attributes):
        table_name = "agente_competidor_importe_nominal_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "PRIMARY KEY (pais_destino, num_agente, divisa, competidor, year, month, day, importe_nominal, importe_destino, timestamp) )" \
                        "WITH CLUSTERING ORDER BY (num_agente DESC, divisa DESC, competidor DESC, year DESC, month DESC, day DESC, importe_nominal DESC, importe_destino DESC, timestamp DESC);"
        return table_query

    ########### CIUDAD ##############
    @staticmethod
    def ciudad(attributes):
        table_name = "ciudad_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "PRIMARY KEY (pais_destino, ciudad, divisa, year, month, day, importe_destino, timestamp) )" \
                        "WITH CLUSTERING ORDER BY (ciudad DESC, divisa DESC, year DESC, month DESC, day DESC, importe_destino DESC, timestamp DESC);"
        return table_query

    @staticmethod
    def ciudad_competidor(attributes):
        table_name = "ciudad_competidor_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "PRIMARY KEY (pais_destino, ciudad, divisa, competidor, year, month, day, importe_destino, timestamp) )" \
                        "WITH CLUSTERING ORDER BY (ciudad DESC, divisa DESC, competidor DESC, year DESC, month DESC, day DESC, importe_destino DESC, timestamp DESC);"
        return table_query

    # This solves the query: Dime la mejor tasa para un dia especifico y para un importe nominal especifico o rango de importes iterando (dia especifico o rango de fechas iterando).
    @staticmethod
    def ciudad_importe_nominal(attributes):
        table_name = "ciudad_importe_nominal_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "PRIMARY KEY (pais_destino, ciudad, divisa, year, month, day, importe_nominal, importe_destino, timestamp) )" \
                        "WITH CLUSTERING ORDER BY (ciudad DESC, divisa DESC, year DESC, month DESC, day DESC, importe_nominal DESC, importe_destino DESC, timestamp DESC);"
        return table_query

    @staticmethod
    def ciudad_competidor_importe_nominal(attributes):
        table_name = "ciudad_competidor_importe_nominal_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "PRIMARY KEY (pais_destino, ciudad, divisa, competidor, year, month, day, importe_nominal, importe_destino, timestamp) )" \
                        "WITH CLUSTERING ORDER BY (ciudad DESC, divisa DESC, competidor DESC, year DESC, month DESC, day DESC, importe_nominal DESC, importe_destino DESC, timestamp DESC);"
        return table_query

    ########### SOLO PAIS_DESTINO ##############
    @staticmethod
    def pais(attributes):
        table_name = "pais_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "PRIMARY KEY (pais_destino, divisa, year, month, day, importe_destino, timestamp) )" \
                        "WITH CLUSTERING ORDER BY (divisa DESC, year DESC, month DESC, day DESC, importe_destino DESC, timestamp DESC);"
        return table_query

    @staticmethod
    def pais_competidor(attributes):
        table_name = "pais_competidor_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "PRIMARY KEY (pais_destino, divisa, competidor, year, month, day, importe_destino, timestamp) )" \
                        "WITH CLUSTERING ORDER BY (divisa DESC, competidor DESC, year DESC, month DESC, day DESC, importe_destino DESC, timestamp DESC);"
        return table_query

    # This solves the query: Dime la mejor tasa para un dia especifico y para un importe nominal especifico o rango de importes iterando (dia especifico o rango de fechas iterando).
    @staticmethod
    def pais_importe_nominal(attributes):
        table_name = "pais_importe_nominal_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "PRIMARY KEY (pais_destino, divisa, year, month, day, importe_nominal, importe_destino, timestamp) )" \
                        "WITH CLUSTERING ORDER BY (divisa DESC, year DESC, month DESC, day DESC, importe_nominal DESC, importe_destino DESC, timestamp DESC);"
        return table_query

    @staticmethod
    def pais_competidor_importe_nominal(attributes):
        table_name = "pais_competidor_importe_nominal_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "PRIMARY KEY (pais_destino, divisa, competidor, year, month, day, importe_nominal, importe_destino, timestamp) )" \
                        "WITH CLUSTERING ORDER BY (divisa DESC, competidor DESC, year DESC, month DESC, day DESC, importe_nominal DESC, importe_destino DESC, timestamp DESC);"
        return table_query


    ########### GEOHASH ##############
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

    @staticmethod
    def geohash(attributes):
        table_name = "geohash_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "geohash text, " \
                      + "PRIMARY KEY (pais_destino, geohash, divisa, year, month, day, importe_destino, timestamp) )" \
                        "WITH CLUSTERING ORDER BY (geohash DESC, divisa DESC, year DESC, month DESC, day DESC, importe_destino DESC, timestamp DESC);"
        return table_query

    @staticmethod
    def geohash_competidor(attributes):
        table_name = "geohash_competidor_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "geohash text, " \
                      + "PRIMARY KEY (pais_destino, geohash, divisa, competidor, year, month, day, importe_destino, timestamp) )" \
                        "WITH CLUSTERING ORDER BY (geohash DESC, divisa DESC, competidor DESC, year DESC, month DESC, day DESC, importe_destino DESC, timestamp DESC);"
        return table_query

    # This solves the query: Dime la mejor tasa para un dia especifico y para un importe nominal especifico o rango de importes iterando (dia especifico o rango de fechas iterando).
    @staticmethod
    def geohash_importe_nominal(attributes):
        table_name = "geohash_importe_nominal_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "geohash text, " \
                      + "PRIMARY KEY (pais_destino, geohash, divisa, year, month, day, importe_nominal, importe_destino, timestamp) )" \
                        "WITH CLUSTERING ORDER BY (geohash DESC, divisa DESC, year DESC, month DESC, day DESC, importe_nominal DESC, importe_destino DESC, timestamp DESC);"
        return table_query

    @staticmethod
    def geohash_competidor_importe_nominal(attributes):
        table_name = "geohash_competidor_importe_nominal_query"
        table_query = "CREATE TABLE " + table_name + "(" \
                      + attributes \
                      + "geohash text, " \
                      + "PRIMARY KEY (pais_destino, geohash, divisa, competidor, year, month, day, importe_nominal, importe_destino, timestamp) )" \
                        "WITH CLUSTERING ORDER BY (geohash DESC, divisa DESC, competidor DESC, year DESC, month DESC, day DESC, importe_nominal DESC, importe_destino DESC, timestamp DESC);"
        return table_query


