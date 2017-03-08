from cassandra_logic import CassandraLogic
from flask import Flask, render_template, request
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


@app.route("/competencia/reset", methods=['GET'])
def reset():
    # Creating a Cassandra Logic object.
    cassandra_init = CassandraLogic('127.0.0.1', 'precios_competencia', True)

    # Connect to the key space.
    cassandra = cassandra_init.connect_keyspace()

    # Drop table.
    cassandra.drop_and_create_table('precios')
    return render_template('reset.html', **locals())


@app.route("/competencia/cassandra", methods=['GET'])
def cassandra():
    # Requesting POST parameters.
    pais = request.args.get('pais')
    codigo_postal = request.args.get('codigoPostal')
    pais_destino = request.args.get('paisDestino')
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

    # Create and drop the key space.
    # cassandra = cassandra_init.drop_and_create_keyspace()
    # Just connect to the key space.
    cassandra = cassandra_init.connect_keyspace()

    # Create and drop the table. Comment if you do not want to reset the table.
    # cassandra.drop_and_create_table('precios')

    # Column names to insert.
    # TODO: just include not empty columns.
    column_names = ["competidor", "codigo_postal", "pais_destino", "pais", "divisa", "importe", "modo_entrega",
                    "canal_captacion", "user", "timestamp", "comision", "tasa_cambio"]

    # Column values to insert.
    # TODO: just include not empty columns.
    column_values = [competidor, codigo_postal, pais_destino, pais, divisa, importe, modo_entrega,
                     canal_captacion, usuario, timestamp, comision, tasa_cambio]
    # Convert from unicode to string.
    column_values = [str(v) for v in column_values]

    # Insert data in table.
    cassandra.insert_data('precios', column_names, column_values)
    return render_template('cassandra.html', **locals())


if __name__ == "__main__":
    app.run(host='127.0.0.2', port=80)