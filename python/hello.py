from cassandra_logic import CassandraLogic
from flask import Flask, render_template, request
from utils.geolocation import GeoLocation

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
    # Creating a Cassandra Logic object.
    cassandra_init = CassandraLogic('127.0.0.1', 'precios_competencia', True)

    # Connect to the key space.
    cassandra = cassandra_init.connect_keyspace()

    rows = cassandra.select_all('query1')
    rows2 = cassandra.select_all('query2')
    return render_template('busqueda.html', **locals())

@app.route("/competencia/reset", methods=['GET'])
def reset():
    # Creating a Cassandra Logic object.
    cassandra_init = CassandraLogic('127.0.0.1', 'precios_competencia', True)

    # Connect to the key space.
    cassandra = cassandra_init.connect_keyspace()

    # Drop table.
    table_names = ["query1", "query2"]
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

    # Creating a Cassandra Logic object.
    cassandra_init = CassandraLogic('127.0.0.1', 'precios_competencia', True)

    # Just connect to the key space.
    cassandra = cassandra_init.connect_keyspace()

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
            "tasa_cambio": tasa_cambio}

    cassandra.insert_into_all_tables(data)
    return render_template('insertar_result.html', **locals())


@app.route("/competencia/query1", methods=['GET'])
def query1():
    return render_template('query1.html', **locals())


@app.route("/competencia/query1_result", methods=['GET'])
def query1_result():
    # Creating a Cassandra Logic object.
    cassandra_init = CassandraLogic('127.0.0.1', 'precios_competencia', True)

    # Just connect to the key space.
    cassandra = cassandra_init.connect_keyspace()

    ciudad = request.args.get('ciudad')
    pais_destino = request.args.get('paisDestino')
    divisa = request.args.get('divisa')
    competidor = request.args.get('competidor')

    results = []
    if competidor == 'Todos':
        results = cassandra.best_tasa_given_divisa('query1', ciudad, pais_destino, divisa)
    else:
        results = cassandra.best_tasa_given_divisa('query2', ciudad, pais_destino, divisa, competidor)

    return render_template('query1_result.html', **locals())

if __name__ == "__main__":
    app.run(host='127.0.0.2', port=80)