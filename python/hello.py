from cassandra_logic import CassandraLogic
from flask import Flask, flash, redirect, render_template, request, session, abort
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

@app.route("/competencia/cassandra", methods=['GET'])
def cassandra():
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

    column_names = ["pais", "codigo_postal", "pais_destino", "competidor", "divisa", "importe", "modo_entrega",
                    "canal_captacion", "user", "timestamp", "comision", "tasa_cambio"]

    column_values = [pais, codigo_postal, pais_destino, competidor, divisa, importe, modo_entrega,
                     canal_captacion, usuario, timestamp, comision, tasa_cambio]
    column_values = [str(v) for v in column_values]

    print(column_values)
    cassandra.insert_data('precios', column_names, column_values)
    return render_template('cassandra.html', **locals())



if __name__ == "__main__":
    app.run(host='127.0.0.2', port=80)