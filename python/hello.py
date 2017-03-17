from cassandra_logic import CassandraLogic
from flask import Flask, render_template, request
from utils.geolocation import GeoLocation
from datetime import date, timedelta as td
from collections import OrderedDict
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
    rows_query7 = cassandra.select_all('ciudad_ts_importe_nominal_query')
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
    day =  request.args.get('day')
    month = request.args.get('month')
    year = request.args.get('year')

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
            "num_agente": num_agente,
            "day": day,
            "month": month,
            "year": year
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
    num_agente = request.args.get('numAgente')

    if ciudad != '':
        key_field = 'ciudad'
    elif num_agente != '':
        key_field = 'agente'
    else:
        key_field = 'pais'

    pais_destino = request.args.get('paisDestino')
    divisa = request.args.get('divisa')
    competidor = request.args.get('competidor')

    importe_nominal = float(str(request.args.get('importeNominal'))) if request.args.get('importeNominal') != '' else None
    min_range_importe_nominal = float(str(request.args.get('minRangeImporteNominal'))) if request.args.get('minRangeImporteNominal') != '' else None
    max_range_importe_nominal = float(str(request.args.get('maxRangeImporteNominal'))) if request.args.get('maxRangeImporteNominal') != '' else None
    range_importe_nominal = [min_range_importe_nominal, max_range_importe_nominal] if min_range_importe_nominal and max_range_importe_nominal else []

    min_date = request.args.get('minDate') if request.args.get('minDate') != '' else None
    max_date = request.args.get('maxDate') if request.args.get('maxDate') != '' else None
    concrete_date = request.args.get('concreteDate') if request.args.get('concreteDate') != '' else None
    day = month = year = delta_date = None

    mostrar = int(request.args.get('mostrar'))

    if min_date and max_date:
        min_date = [int(i) for i in min_date.split("/")]
        min_date = date(min_date[2], min_date[1], min_date[0])
        max_date = [int(i) for i in max_date.split("/")]
        max_date = date(max_date[2], max_date[1], max_date[0])
        delta_date = max_date - min_date
        intermediate_mostrar = -1
    elif concrete_date:
        day, month, year = [int(i) for i in concrete_date.split("/")]
        min_date = date(year, month, day)
        delta_date = td(0)

    competidor = None if competidor == 'Todos' else competidor

    results = []
    upper_value = None
    upper_tasa_results = []
    lower_value = None
    lower_tasa_results = []
    importe_destino_id = 6 if competidor else 7
    importe_destino_id = importe_destino_id-1 if key_field=='pais' else importe_destino_id
    table = ""
    matched_specific_result = False

    if importe_nominal or range_importe_nominal or delta_date:
        table = key_field+"_competidor_importe_nominal_query" if competidor else key_field+"_importe_nominal_query"
        if delta_date: # We cannot limit the individuals queries, but the overall query later instead.
            mostrar = -1
    else:
        table = key_field+"_competidor_query" if competidor else key_field+"_query"

    for i in range(delta_date.days + 1):
        fecha = min_date + td(days=i)
        day, month, year = fecha.day, fecha.month, fecha.year

        specific_day_results = cassandra.best_tasa(table, pais_destino, divisa,
                                ciudad=ciudad,
                                num_agente=num_agente,
                                competidor=competidor,
                                day=day,
                                month=month,
                                year=year,
                                importe_nominal=importe_nominal,
                                range_importe_nominal=range_importe_nominal,
                                )

        if specific_day_results:
            matched_specific_result = True

        # Specific importe: There does not exist a specific match in importe_nominal.
        if not specific_day_results and importe_nominal and not matched_specific_result:
            lower_value = cassandra.best_tasa(table, pais_destino, divisa,
                                              ciudad=ciudad,
                                              num_agente=num_agente,
                                              competidor=competidor,
                                              day=day,
                                              month=month,
                                              year=year,
                                              importe_nominal=importe_nominal,
                                              search='lower',
                                              )
            lower_value = float(lower_value[0][0]) if lower_value else None
            lower_value = importe_nominal - lower_value if lower_value and abs(lower_value) < 10000000.0 else None

            upper_value = cassandra.best_tasa(table, pais_destino, divisa,
                                              ciudad=ciudad,
                                              num_agente=num_agente,
                                              competidor=competidor,
                                              day=day,
                                              month=month,
                                              year=year,
                                              importe_nominal=importe_nominal,
                                              search='upper',
                                              )
            upper_value = float(upper_value[0][0]) if upper_value else None
            upper_value = upper_value + importe_nominal if upper_value and upper_value < 10000000.0 else None

            if lower_value:
                lower_tasa_results = cassandra.best_tasa(table, pais_destino, divisa,
                                                         ciudad=ciudad,
                                                         num_agente=num_agente,
                                                         competidor=competidor,
                                                         day=day,
                                                         month=month,
                                                         year=year,
                                                         importe_nominal=lower_value,
                                                         )
            if upper_value:
                upper_tasa_results = cassandra.best_tasa(table, pais_destino, divisa,
                                ciudad=ciudad,
                                num_agente=num_agente,
                                competidor=competidor,
                                day=day,
                                month=month,
                                year=year,
                                importe_nominal=upper_value,
                                )

        for s in specific_day_results:
            results.append(s)

    # Ranking the results
    results_dict = dict()
    for cont, u in enumerate(results):
        results_dict[cont] = u
    ordered_results = OrderedDict(
        sorted(results_dict.items(), key=lambda t: float(t[1][importe_destino_id]), reverse=True))

    results = ordered_results.values()[:mostrar]
    return render_template('query1_result.html', **locals())


@app.route("/competencia/query2", methods=['GET'])
def query2():
    return render_template('query2.html', **locals())


@app.route("/competencia/query2_result", methods=['GET'])
def query2_result():
    cassandra = CassandraLogic.from_existing_keyspace('127.0.0.1', 'precios_competencia')

    lat = request.args.get('lat')
    lon = request.args.get('lon')
    loc = GeoLocation.from_degrees(float(lat), float(lon))

    distancia = request.args.get('distancia')
    distance = float(distancia)  # 1 kilometer
    bs_min, bs_max = loc.bounding_locations(distance)
    geohash_max = Geohash.encode(bs_max.deg_lat, bs_max.deg_lon)
    geohash_min = Geohash.encode(bs_min.deg_lat, bs_min.deg_lon)
    geohash_range = [geohash_max, geohash_min]

    key_field = 'geohash'

    pais_destino = request.args.get('paisDestino')
    divisa = request.args.get('divisa')
    competidor = request.args.get('competidor')

    importe_nominal = float(str(request.args.get('importeNominal'))) if request.args.get(
        'importeNominal') != '' else None
    min_range_importe_nominal = float(str(request.args.get('minRangeImporteNominal'))) if request.args.get(
        'minRangeImporteNominal') != '' else None
    max_range_importe_nominal = float(str(request.args.get('maxRangeImporteNominal'))) if request.args.get(
        'maxRangeImporteNominal') != '' else None
    range_importe_nominal = [min_range_importe_nominal,
                             max_range_importe_nominal] if min_range_importe_nominal and max_range_importe_nominal else []

    min_date = request.args.get('minDate') if request.args.get('minDate') != '' else None
    max_date = request.args.get('maxDate') if request.args.get('maxDate') != '' else None
    concrete_date = request.args.get('concreteDate') if request.args.get('concreteDate') != '' else None
    day = month = year = delta_date = None

    mostrar = int(request.args.get('mostrar'))

    if min_date and max_date:
        min_date = [int(i) for i in min_date.split("/")]
        min_date = date(min_date[2], min_date[1], min_date[0])
        max_date = [int(i) for i in max_date.split("/")]
        max_date = date(max_date[2], max_date[1], max_date[0])
        delta_date = max_date - min_date
    elif concrete_date:
        day, month, year = [int(i) for i in concrete_date.split("/")]
        min_date = date(year, month, day)
        delta_date = td(0)

    competidor = None if competidor == 'Todos' else competidor

    results = []
    upper_value = None
    upper_tasa_results = []
    lower_value = None
    lower_tasa_results = []
    importe_destino_id = 6 if competidor else 7
    importe_destino_id = importe_destino_id - 1 if key_field == 'pais' else importe_destino_id
    geohash_id = 3 if competidor else 2
    table = ""

    if importe_nominal or range_importe_nominal or delta_date:
        table = key_field + "_competidor_importe_nominal_query" if competidor else key_field + "_importe_nominal_query"
    else:
        table = key_field + "_competidor_query" if competidor else key_field + "_query"


    geohash_scheme_table = "geohash_competidor_scheme" if competidor else "geohash_scheme"

    geolocations = []
    geolocations = cassandra.best_tasa(geohash_scheme_table, pais_destino, divisa,
                                                    geohash_range=geohash_range,
                                                    competidor=competidor,
                                                    )
    matched_specific_result = False
    for g in geolocations:
        row_geohash = g[geohash_id]

        for i in range(delta_date.days + 1):
            fecha = min_date + td(days=i)
            day, month, year = fecha.day, fecha.month, fecha.year

            specific_day_results = cassandra.best_tasa(table, pais_destino, divisa,
                                                       geohash=row_geohash,
                                                       competidor=competidor,
                                                       day=day,
                                                       month=month,
                                                       year=year,
                                                       importe_nominal=importe_nominal,
                                                       range_importe_nominal=range_importe_nominal,
                                                       )

            if specific_day_results and not matched_specific_result:
                matched_specific_result = True

            # Specific importe: There does not exist a specific match in importe_nominal.
            if not specific_day_results and importe_nominal and not matched_specific_result:
                lower_value = cassandra.best_tasa(table, pais_destino, divisa,
                                                  geohash=row_geohash,
                                                  competidor=competidor,
                                                  day=day,
                                                  month=month,
                                                  year=year,
                                                  importe_nominal=importe_nominal,
                                                  search='lower',
                                                  )
                lower_value = float(lower_value[0][0]) if lower_value else None
                lower_value = importe_nominal - lower_value if lower_value and abs(lower_value) < 10000000.0 else None

                upper_value = cassandra.best_tasa(table, pais_destino, divisa,
                                                  geohash=row_geohash,
                                                  competidor=competidor,
                                                  day=day,
                                                  month=month,
                                                  year=year,
                                                  importe_nominal=importe_nominal,
                                                  search='upper',
                                                  )
                upper_value = float(upper_value[0][0]) if upper_value else None
                upper_value = upper_value + importe_nominal if upper_value and upper_value < 10000000.0 else None

                if lower_value:
                    lower_tasa_specific_results = cassandra.best_tasa(table, pais_destino, divisa,
                                                             geohash=row_geohash,
                                                             competidor=competidor,
                                                             day=day,
                                                             month=month,
                                                             year=year,
                                                             importe_nominal=lower_value,
                                                             )
                    for l in lower_tasa_specific_results:
                        lower_tasa_results.append(l)
                if upper_value:
                    upper_tasa_specific_results = cassandra.best_tasa(table, pais_destino, divisa,
                                                             geohash=row_geohash,
                                                             competidor=competidor,
                                                             day=day,
                                                             month=month,
                                                             year=year,
                                                             importe_nominal=upper_value,
                                                           )
                    for t in upper_tasa_specific_results:
                        upper_tasa_results.append(t)

            for s in specific_day_results:
                results.append(s)

    # Ranking the results
    results_dict = dict()
    for cont, u in enumerate(results):
        results_dict[cont] = u
    ordered_results = OrderedDict(
        sorted(results_dict.items(), key=lambda t: float(t[1][importe_destino_id]), reverse=True))

    results = ordered_results.values()[:mostrar]

    # Upper tasa if exact import does not exist in DB.
    upper_tasa_results_dict = dict()
    for cont, u in enumerate(upper_tasa_results):
        upper_tasa_results_dict[cont] = u
    ordered_upper_tasa_results = OrderedDict(
         sorted(upper_tasa_results_dict.items(), key=lambda t: float(t[1][importe_destino_id]), reverse=True))

    upper_tasa_results = ordered_upper_tasa_results.values()[:mostrar]

    # Lower tasa if exact import does not exist in DB.
    lower_tasa_results_dict = dict()
    for cont, u in enumerate(lower_tasa_results):
        lower_tasa_results_dict[cont] = u
    ordered_lower_tasa_results = OrderedDict(
        sorted(lower_tasa_results_dict.items(), key=lambda t: float(t[1][importe_destino_id ]), reverse=True))

    lower_tasa_results = ordered_lower_tasa_results.values()[:mostrar]
    return render_template('query2_result.html', **locals())

if __name__ == "__main__":
    app.run(host='127.0.0.2', port=80)