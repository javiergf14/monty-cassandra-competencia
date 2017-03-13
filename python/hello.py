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

    rows = cassandra.select_all('query1')
    rows2 = cassandra.select_all('query3')
    return render_template('busqueda.html', **locals())


@app.route("/competencia/reset", methods=['GET'])
def reset():
    # Creating a Cassandra Logic object.
    cassandra_reset = CassandraLogic('127.0.0.1', 'precios_competencia')
    cassandra_reset.drop_and_create_keyspace()

    # Connect to the key space.
    cassandra = CassandraLogic.from_existing_keyspace('127.0.0.1', 'precios_competencia')

    # Drop table.
    table_names = ["query1", "query2", "query3", "query4"]
    cassandra.drop_and_create_tables(table_names)
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
    importe = request.args.get('importe')
    modo_entrega = request.args.get('modoEntrega')
    canal_captacion = request.args.get('canalCaptacion')
    usuario = request.args.get('usuario')
    timestamp = int(time.time())
    comision = request.args.get('comision')
    tasa_cambio = request.args.get('tasaCambio')
    lat = request.args.get('lat')
    lon = request.args.get('lon')

    cassandra = CassandraLogic.from_existing_keyspace('127.0.0.1', 'precios_competencia')

    data = {"pais": pais,
            "codigo_postal": codigo_postal,
            "pais_destino": pais_destino,
            "ciudad": ciudad,
            "competidor": competidor,
            "divisa": divisa,
            "importe": importe,
            "modo_entrega": modo_entrega,
            "canal_captacion": canal_captacion,
            "usuario": usuario,
            "timestamp": timestamp,
            "comision": comision,
            "tasa_cambio": tasa_cambio,
            "lat": lat,
            "lon": lon
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

    results = []
    if competidor == 'Todos':
        results = cassandra.best_tasa_given_ciudad('query1', ciudad, pais_destino, divisa)
    else:
        results = cassandra.best_tasa_given_divisa('query2', ciudad, pais_destino, divisa, competidor)

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

    lat_max = bs_max.deg_lat
    lon_max = bs_max.deg_lon
    lat_min = bs_min.deg_lat
    lon_min = bs_min.deg_lon

    geohash_max = Geohash.encode(lat_max, lon_max)
    geohash_min = Geohash.encode(lat_min, lon_min)

    print(lat_max, lon_max)
    print(lat_min, lon_min)
    print(bs_min)
    results = []
    if competidor == 'Todos':
        results = cassandra.best_tasa_given_coordinates('query3', pais_destino, divisa, geohash_max, geohash_min, mostrar)
    else:
        results = cassandra.best_tasa_given_coordinates('query4', pais_destino, divisa, geohash_max, geohash_min,
                                                        mostrar, competidor)

    return render_template('query2_result.html', **locals())


if __name__ == "__main__":
    app.run(host='127.0.0.2', port=80)