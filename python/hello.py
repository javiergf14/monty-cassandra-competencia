from cassandra_logic import CassandraLogic
from flask import Flask, render_template, request
from utils.geolocation import GeoLocation
import Geohash

import time

# FLASK section.
app = Flask(__name__)


@app.route("/")
def index():
    return "Competencia Flask App!"


@app.route("/competencia/", methods=['GET'])
def initial_page():
    return render_template('inicio.html', **locals())


@app.route("/competencia/insertar", methods=['GET'])
def insertar():
    return render_template('insertar.html', **locals())


@app.route("/competencia/busqueda", methods=['GET'])
def busqueda():
    # Connect to the key space.
    cassandra = CassandraLogic.from_existing_keyspace('127.0.0.1', 'precios_competencia')

    rows_query1 = cassandra.select_all('ciudad_query')
    rows_query3 = cassandra.select_all('geohash_query')
    rows_query5 = cassandra.select_all('agente_query')
    rows_query7 = cassandra.select_all('ciudad_timestamp_importe_nominal_query')
    return render_template('busqueda.html', **locals())


@app.route("/competencia/reset", methods=['GET'])
def reset():
    # Creating a Cassandra Logic object.
    cassandra_reset = CassandraLogic('127.0.0.1', 'precios_competencia')
    cassandra_reset.drop_and_create_keyspace()

    # Connect to the key space.
    cassandra = CassandraLogic.from_existing_keyspace('127.0.0.1', 'precios_competencia')

    # Drop table.
    cassandra.drop_and_create_tables()
    return render_template('reset.html', **locals())


@app.route("/competencia/insertar_result", methods=['GET'])
def insertar_result():
    # Requesting POST parameters.
    pais = request.args.get('pais')
    codigo_postal = request.args.get('codigoPostal')
    pais_destino = request.args.get('paisDestino')
    ciudad = request.args.get('ciudad')
    competidor = request.args.get('competidor')
    divisa = request.args.get('divisa')
    importe_nominal = request.args.get('importeNominal')
    modo_entrega = request.args.get('modoEntrega')
    canal_captacion = request.args.get('canalCaptacion')
    usuario = request.args.get('usuario')
    timestamp = int(time.time())
    comision = request.args.get('comision')
    tasa_cambio = request.args.get('tasaCambio')
    lat = request.args.get('lat')
    lon = request.args.get('lon')
    num_agente = request.args.get('numAgente')

    cassandra = CassandraLogic.from_existing_keyspace('127.0.0.1', 'precios_competencia')

    data = {"pais": pais,
            "codigo_postal": codigo_postal,
            "pais_destino": pais_destino,
            "ciudad": ciudad,
            "competidor": competidor,
            "divisa": divisa,
            "importe_nominal": importe_nominal,
            "modo_entrega": modo_entrega,
            "canal_captacion": canal_captacion,
            "usuario": usuario,
            "timestamp": timestamp,
            "comision": comision,
            "tasa_cambio": tasa_cambio,
            "lat": lat,
            "lon": lon,
            "num_agente": num_agente
            }

    cassandra.insert_into_all_tables(data)
    return render_template('insertar_result.html', **locals())


@app.route("/competencia/query1", methods=['GET'])
def query1():
    return render_template('query1.html', **locals())


@app.route("/competencia/query1_result", methods=['GET'])
def query1_result():
    cassandra = CassandraLogic.from_existing_keyspace('127.0.0.1', 'precios_competencia')

    ciudad = request.args.get('ciudad')
    pais_destino = request.args.get('paisDestino')
    divisa = request.args.get('divisa')
    competidor = request.args.get('competidor')
    mostrar = request.args.get('mostrar')
    min_ts = request.args.get('minTS')
    max_ts = request.args.get('maxTS')
    importe_nominal = request.args.get('importeNominal')

    timestamp = []
    if max_ts != '' and min_ts != '':
        timestamp = [max_ts, min_ts]

    results = []
    upper_tasa = []
    upper_value = 0
    lower_tasa = []
    lower_value = 0

    if competidor == 'Todos': # All the competitors
        if timestamp: # Timestamp range
            if importe_nominal == 'Cualquiera':
                results = cassandra.best_tasa('ciudad_timestamp_importe_query', pais_destino, divisa,
                                                ciudad=ciudad,
                                                timestamp=timestamp,
                                                alt_table='ciudad_importe_timestamp_query',
                                                mostrar=mostrar)
            else:
                results = cassandra.best_tasa('ciudad_importe_nominal_timestamp_query', pais_destino, divisa,
                                                ciudad=ciudad,
                                                timestamp=timestamp,
                                                importe_nominal=importe_nominal,
                                                alt_table='ciudad_importe_nominal_y_destino_timestamp_query',
                                                mostrar=mostrar)
                if not results: # Get best tasa inferior y superior.
                    lower_value = cassandra.best_tasa('ciudad_timestamp_importe_nominal_query', pais_destino, divisa,
                                                ciudad=ciudad,
                                                timestamp=timestamp,
                                                importe_nominal=importe_nominal,
                                                search='lower',
                                                mostrar=mostrar)


                    upper_value = cassandra.best_tasa('ciudad_timestamp_importe_nominal_query', pais_destino, divisa,
                                                      ciudad=ciudad,
                                                      timestamp=timestamp,
                                                      importe_nominal=importe_nominal,
                                                      search='upper',
                                                      mostrar=mostrar)
                    if lower_value:
                        lower_value = float(str(importe_nominal)) - float(lower_value[0][0])
                        print('im here')
                        lower_tasa = cassandra.best_tasa('ciudad_importe_nominal_timestamp_query', pais_destino, divisa,
                                                    ciudad=ciudad,
                                                    timestamp=timestamp,
                                                    importe_nominal=lower_value,
                                                    search='pe',
                                                    mostrar=mostrar)

                    if upper_value:
                        upper_value = float(str(importe_nominal)) + float(upper_value[0][0])
                        upper_tasa = cassandra.best_tasa('ciudad_importe_nominal_timestamp_query', pais_destino, divisa,
                                                        ciudad=ciudad,
                                                        timestamp=timestamp,
                                                        importe_nominal=upper_value,
                                                        search='pe',
                                                        mostrar=mostrar)

        else:
            results = cassandra.best_tasa('ciudad_query', pais_destino, divisa,
                                          ciudad=ciudad,
                                          mostrar=mostrar)
    else: # Specific competitor
        if timestamp: # Timestamp range
            results = cassandra.best_tasa('ciudad_competidor_timestamp_query', pais_destino, divisa,
                                          ciudad=ciudad,
                                          timestamp=timestamp,
                                          alt_table='ciudad_competidor_importe_timestamp_query',
                                          mostrar=mostrar)
        else:
            results = cassandra.best_tasa('ciudad_competidor_query', pais_destino, divisa,
                                          ciudad=ciudad,
                                          competidor=competidor,
                                          mostrar=mostrar)

    return render_template('query1_result.html', **locals())

@app.route("/competencia/query2", methods=['GET'])
def query2():
    return render_template('query2.html', **locals())


@app.route("/competencia/query2_result", methods=['GET'])
def query2_result():
    cassandra = CassandraLogic.from_existing_keyspace('127.0.0.1', 'precios_competencia')

    lat = request.args.get('lat')
    lon = request.args.get('lon')
    pais_destino = request.args.get('paisDestino')
    divisa = request.args.get('divisa')
    competidor = request.args.get('competidor')
    distancia = request.args.get('distancia')
    mostrar = request.args.get('mostrar')

    loc = GeoLocation.from_degrees(float(lat), float(lon))
    distance = float(distancia) # 1 kilometer
    bs_min, bs_max = loc.bounding_locations(distance)

    geohash_max = Geohash.encode(bs_max.deg_lat,  bs_max.deg_lon)
    geohash_min = Geohash.encode(bs_min.deg_lat, bs_min.deg_lon)

    geohash = [geohash_max, geohash_min]

    results = []
    if competidor == 'Todos':
        results = cassandra.best_tasa('geohash_query', pais_destino, divisa,
                                                        geohash=geohash,
                                                        mostrar=mostrar)
    else:
        results = cassandra.best_tasa('geohash_competidor_query', pais_destino, divisa,
                                                        geohash=geohash,
                                                        competidor=competidor,
                                                        mostrar=mostrar)

    return render_template('query2_result.html', **locals())


@app.route("/competencia/query3", methods=['GET'])
def query3():
    return render_template('query3.html', **locals())


@app.route("/competencia/query3_result", methods=['GET'])
def query3_result():
    cassandra = CassandraLogic.from_existing_keyspace('127.0.0.1', 'precios_competencia')

    num_agente = request.args.get('numAgente')
    pais_destino = request.args.get('paisDestino')
    divisa = request.args.get('divisa')
    competidor = request.args.get('competidor')
    mostrar = request.args.get('mostrar')

    results = []
    if competidor == 'Todos':
        results = cassandra.best_tasa('agente_query',pais_destino, divisa,
                                                   num_agente=num_agente,
                                                   mostrar=mostrar)
    else:
        results = cassandra.best_tasa('agente_competidor_query',pais_destino, divisa,
                                                   num_agente=num_agente,
                                                   competidor=competidor,
                                                   mostrar=mostrar)

    return render_template('query3_result.html', **locals())

if __name__ == "__main__":
    app.run(host='127.0.0.2', port=80)