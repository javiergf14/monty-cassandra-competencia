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
    pais_destino = request.args.get('paisDestino')
    divisa = request.args.get('divisa')
    competidor = request.args.get('competidor')
    mostrar = request.args.get('mostrar')
    min_ts = request.args.get('minTS')
    max_ts = request.args.get('maxTS')
    importe_nominal = request.args.get('importeNominal')
    min_date = request.args.get('minDate')
    max_date = request.args.get('maxDate')
    min_range_importe_nominal = request.args.get('minRangeImporteNominal')
    max_range_importe_nominal = request.args.get('maxRangeImporteNominal')

    importe_nominal_id = 7

    key_fields = ["ciudad", "agente"]
    key_id = -1
    if ciudad == '':
        ciudad = []
        key_id = 1
    elif num_agente == '':
        num_agente = []
        key_id = 0
    else:
        raise Exception('Either ciudad or num_agente must be filled!')

    if min_date != '' and max_date != '':
        min_date = min_date.split("/")
        max_date = max_date.split("/")

        min_date = [int(i) for i in min_date]
        max_date = [int(i) for i in max_date]

    range_importe_nominal = []
    if min_range_importe_nominal != '' and max_range_importe_nominal != '':
        range_importe_nominal = [int(min_range_importe_nominal), int(max_range_importe_nominal)]

    delta1 = []
    if min_date and max_date and range_importe_nominal:
        date1 = date(min_date[2], min_date[1], min_date[0])
        date2 = date(max_date[2], max_date[1], max_date[0])
        delta1 = date2 - date1

    timestamp = []
    if max_ts != '' and min_ts != '':
        timestamp = [max_ts, min_ts]

    results = []
    upper_tasa = []
    upper_value = 0
    lower_tasa = []
    lower_value = 0

    competidor_flag = True
    if competidor == 'Todos':
        competidor = []
        competidor_flag = False
        importe_nominal_id -= 1


    fecha_range_table = key_fields[key_id]+competidor_flag*"_competidor"+"_fecha_importe_nominal_query"
    fecha_range_alt_table = key_fields[key_id]+competidor_flag*"_competidor"+"_fecha_importe_destino_query"

    timestamp_all_importes_table = key_fields[key_id]+competidor_flag*"_competidor"+"_ts_importe_query"
    timestamp_all_importes_alt_table = key_fields[key_id]+competidor_flag*"_competidor"+"_importe_ts_query"

    exact_importe_table = key_fields[key_id]+competidor_flag*"_competidor"+"_importe_nominal_ts_query"
    exact_importe_alt_table = key_fields[key_id]+competidor_flag*"_competidor"+"_importes_ts_query"
    exact_importe_limits_table = key_fields[key_id]+competidor_flag*"_competidor"+"_ts_importe_nominal_query"

    no_ts_table = key_fields[key_id]+competidor_flag*"_competidor"+"_query"

    # Fecha range.
    if range_importe_nominal:
        candidate_solutions = dict()
        for i in range(delta1.days + 1):
            fecha = date1 + td(days=i)
            results = cassandra.best_tasa(fecha_range_table, pais_destino, divisa,
                                          ciudad=ciudad,
                                          num_agente=num_agente,
                                          competidor=competidor,
                                          range_importe_nominal=range_importe_nominal,
                                          year=fecha.year,
                                          month=fecha.month,
                                          day=fecha.day,
                                          alt_table=fecha_range_alt_table)
            if results:
                candidate_solutions[results[0][importe_nominal_id]] = results

        sorted_candidates = OrderedDict(sorted(candidate_solutions.items(), key=lambda t: t[0], reverse=True))
        try:
            results = sorted_candidates.items()[0]
        except IndexError:
            # results = ["No existe correspondencia con parametros filtrados"]
            results = []

    # Timestamp range
    elif timestamp:
        # All the importes.
        if importe_nominal == 'Cualquiera':
            results = cassandra.best_tasa(timestamp_all_importes_table, pais_destino, divisa,
                                          ciudad=ciudad,
                                          num_agente=num_agente,
                                          competidor=competidor,
                                          timestamp=timestamp,
                                          alt_table=timestamp_all_importes_alt_table,
                                          mostrar=mostrar)
        # Exact importe.
        else:
            results = cassandra.best_tasa(exact_importe_table, pais_destino, divisa,
                                          ciudad=ciudad,
                                          competidor=competidor,
                                          num_agente=num_agente,
                                          timestamp=timestamp,
                                          importe_nominal=importe_nominal,
                                          alt_table=exact_importe_alt_table,
                                          mostrar=mostrar)
            # If exact importe does not exist, take the upper importe and the lower importe.
            if not results:
                lower_value = cassandra.best_tasa(exact_importe_limits_table, pais_destino, divisa,
                                                  ciudad=ciudad,
                                                  num_agente=num_agente,
                                                  competidor=competidor,
                                                  timestamp=timestamp,
                                                  importe_nominal=importe_nominal,
                                                  search='lower',
                                                  mostrar=mostrar)

                upper_value = cassandra.best_tasa(exact_importe_limits_table, pais_destino, divisa,
                                                  ciudad=ciudad,
                                                  num_agente=num_agente,
                                                  competidor=competidor,
                                                  timestamp=timestamp,
                                                  importe_nominal=importe_nominal,
                                                  search='upper',
                                                  mostrar=mostrar)
                if lower_value:
                    lower_value = float(str(importe_nominal)) - float(lower_value[0][0])
                    lower_tasa = cassandra.best_tasa(exact_importe_table, pais_destino, divisa,
                                                     ciudad=ciudad,
                                                     num_agente=num_agente,
                                                     competidor=competidor,
                                                     timestamp=timestamp,
                                                     importe_nominal=lower_value,
                                                     search='approx',
                                                     mostrar=mostrar)

                if upper_value:
                    upper_value = float(str(importe_nominal)) + float(upper_value[0][0])
                    upper_tasa = cassandra.best_tasa(exact_importe_table, pais_destino, divisa,
                                                     ciudad=ciudad,
                                                     num_agente=num_agente,
                                                     competidor=competidor,
                                                     timestamp=timestamp,
                                                     importe_nominal=upper_value,
                                                     search='approx',
                                                     mostrar=mostrar)
    # No timestamp range
    else:
        results = cassandra.best_tasa(no_ts_table, pais_destino, divisa,
                                      ciudad=ciudad,
                                      num_agente=num_agente,
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

    min_ts = request.args.get('minTS')
    max_ts = request.args.get('maxTS')
    importe_nominal = request.args.get('importeNominal')
    min_date = request.args.get('minDate')
    max_date = request.args.get('maxDate')
    min_range_importe_nominal = request.args.get('minRangeImporteNominal')
    max_range_importe_nominal = request.args.get('maxRangeImporteNominal')

    loc = GeoLocation.from_degrees(float(lat), float(lon))
    distance = float(distancia) # 1 kilometer
    bs_min, bs_max = loc.bounding_locations(distance)

    geohash_max = Geohash.encode(bs_max.deg_lat,  bs_max.deg_lon)
    geohash_min = Geohash.encode(bs_min.deg_lat, bs_min.deg_lon)

    geohash_range = [geohash_max, geohash_min]

    if min_date != '' and max_date != '':
        min_date = min_date.split("/")
        max_date = max_date.split("/")

        min_date = [int(i) for i in min_date]
        max_date = [int(i) for i in max_date]

    range_importe_nominal = []
    if min_range_importe_nominal != '' and max_range_importe_nominal != '':
        range_importe_nominal = [int(min_range_importe_nominal), int(max_range_importe_nominal)]

    delta1 = []
    if min_date and max_date and range_importe_nominal:
        date1 = date(min_date[2], min_date[1], min_date[0])
        date2 = date(max_date[2], max_date[1], max_date[0])
        delta1 = date2 - date1

    timestamp = []
    if max_ts != '' and min_ts != '':
        timestamp = [max_ts, min_ts]

    importe_nominal_id = 6
    geohash_id = 3
    competidor_flag = True
    if competidor == 'Todos':
        competidor = []
        competidor_flag = False
        geohash_id -= 1

    geohash_scheme_table = "geohash" + competidor_flag * "_competidor" + "_scheme"

    geolocations = []
    geolocations = cassandra.best_tasa(geohash_scheme_table, pais_destino, divisa,
                                                    geohash_range=geohash_range,
                                                    competidor=competidor,
                                                    mostrar=mostrar)

    fecha_range_table = "geohash" + competidor_flag * "_competidor" + "_fecha_importe_nominal_query"
    fecha_range_alt_table = "geohash" + competidor_flag * "_competidor" + "_fecha_importe_destino_query"

    timestamp_all_importes_table = "geohash" + competidor_flag * "_competidor" + "_ts_importe_query"
    timestamp_all_importes_alt_table = "geohash" + competidor_flag * "_competidor" + "_importe_ts_query"

    exact_importe_table = "geohash" + competidor_flag * "_competidor" + "_importe_nominal_ts_query"
    exact_importe_alt_table = "geohash" + competidor_flag * "_competidor" + "_importes_ts_query"
    exact_importe_limits_table = "geohash" + competidor_flag * "_competidor" + "_ts_importe_nominal_query"

    no_ts_table = "geohash" + competidor_flag * "_competidor" + "_query"

    results = dict()
    for g in geolocations:
        row_geohash = g[geohash_id]

        # Fecha range.
        if range_importe_nominal:
            candidate_solutions = dict()
            for i in range(delta1.days + 1):
                fecha = date1 + td(days=i)
                matches = cassandra.best_tasa(fecha_range_table, pais_destino, divisa,
                                              geohash=row_geohash,
                                              competidor=competidor,
                                              range_importe_nominal=range_importe_nominal,
                                              year=fecha.year,
                                              month=fecha.month,
                                              day=fecha.day,
                                              alt_table=fecha_range_alt_table)
                if matches:
                    candidate_solutions[matches[0][importe_nominal_id]] = matches

            sorted_candidates = OrderedDict(sorted(candidate_solutions.items(), key=lambda t: t[0], reverse=True))
            try:
                best_result = sorted_candidates.items()[0]
                results[best_result[0]] = best_result[1]
            except IndexError:
                # results = ["No existe correspondencia con parametros filtrados"]
                pass

                # Timestamp range
        elif timestamp:
            # All the importes.
            if importe_nominal == 'Cualquiera':
                matches = cassandra.best_tasa(timestamp_all_importes_table, pais_destino, divisa,
                                              geohash=row_geohash,
                                              competidor=competidor,
                                              timestamp=timestamp,
                                              alt_table=timestamp_all_importes_alt_table,
                                              mostrar=mostrar)
                results[matches[0][3]] = matches
            # Exact importe.
            else:
                matches = cassandra.best_tasa(exact_importe_table, pais_destino, divisa,
                                              geohash=row_geohash,
                                              competidor=competidor,
                                              timestamp=timestamp,
                                              importe_nominal=importe_nominal,
                                              alt_table=exact_importe_alt_table,
                                              mostrar=mostrar)
                # If exact importe does not exist, take the upper importe and the lower importe.
                if not matches:
                    lower_value = cassandra.best_tasa(exact_importe_limits_table, pais_destino, divisa,
                                                      geohash=row_geohash,
                                                      competidor=competidor,
                                                      timestamp=timestamp,
                                                      importe_nominal=importe_nominal,
                                                      search='lower',
                                                      mostrar=mostrar)

                    upper_value = cassandra.best_tasa(exact_importe_limits_table, pais_destino, divisa,
                                                      geohash=row_geohash,
                                                      competidor=competidor,
                                                      timestamp=timestamp,
                                                      importe_nominal=importe_nominal,
                                                      search='upper',
                                                      mostrar=mostrar)
                    if lower_value:
                        lower_value = float(str(importe_nominal)) - float(lower_value[0][0])
                        lower_tasa = cassandra.best_tasa(exact_importe_table, pais_destino, divisa,
                                                         geohash=row_geohash,
                                                         competidor=competidor,
                                                         timestamp=timestamp,
                                                         importe_nominal=lower_value,
                                                         search='approx',
                                                         mostrar=mostrar)

                    if upper_value:
                        upper_value = float(str(importe_nominal)) + float(upper_value[0][0])
                        upper_tasa = cassandra.best_tasa(exact_importe_table, pais_destino, divisa,
                                                         geohash=row_geohash,
                                                         competidor=competidor,
                                                         timestamp=timestamp,
                                                         importe_nominal=upper_value,
                                                         search='approx',
                                                         mostrar=mostrar)
                else:
                    results[matches[0][4]] = matches
                    # No timestamp range
        else:
            matches = cassandra.best_tasa(no_ts_table, pais_destino, divisa,
                                          geohash=row_geohash,
                                          competidor=competidor,
                                          mostrar=mostrar)
            print(matches)
            for m in matches:
                results[m[3]] = m
    sorted_results = OrderedDict(sorted(results.items(), key=lambda  t: t[0], reverse=True))
    results = sorted_results.items()[0]



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

    importe_nominal = float(str(request.args.get('importeNominal'))) if request.args.get('importeNominal') != '' else None
    min_range_importe_nominal = float(str(request.args.get('minRangeImporteNominal'))) if request.args.get('minRangeImporteNominal') != '' else None
    max_range_importe_nominal = float(str(request.args.get('maxRangeImporteNominal'))) if request.args.get('maxRangeImporteNominal') != '' else None
    range_importe_nominal = [min_range_importe_nominal, max_range_importe_nominal] if min_range_importe_nominal and max_range_importe_nominal else []

    min_date = request.args.get('minDate') if request.args.get('minDate') != '' else None
    max_date = request.args.get('maxDate') if request.args.get('maxDate') != '' else None
    concrete_date = request.args.get('concreteDate') if request.args.get('concreteDate') != '' else None
    day = month = year = delta_date = None

    mostrar = int(request.args.get('mostrar'))
    last_mostrar = intermediate_mostrar = mostrar

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
    importe_destino_id = 8 if competidor else 7
    table = ""

    if importe_nominal or range_importe_nominal or delta_date:
        table = "agente_competidor_importe_nominal_query" if competidor else "agente_importe_nominal_query"
        if range_importe_nominal or delta_date: # We cannot limit the individuals queries, but the overall query later instead.
            mostrar = -1
    else:
        table = "agente_competidor_query" if competidor else "agente_query"

    for i in range(delta_date.days + 1):
        fecha = min_date + td(days=i)
        day, month, year = fecha.day, fecha.month, fecha.year

        specific_day_results = cassandra.best_tasa(table, pais_destino, divisa,
                                num_agente=num_agente,
                                competidor=competidor,
                                day=day,
                                month=month,
                                year=year,
                                importe_nominal=importe_nominal,
                                range_importe_nominal=range_importe_nominal,
                                mostrar=mostrar,)


        # Range importe: We need to sort the results by importe_destino
        if specific_day_results and range_importe_nominal:
            specific_results_dict = dict()
            for cont, r in enumerate(specific_day_results):
                specific_results_dict[cont] = r

            specific_ordered_results = OrderedDict(sorted(specific_results_dict.items(), key=lambda t: t[1][importe_destino_id], reverse=True))
            specific_day_results = list(specific_ordered_results.values())[:intermediate_mostrar]

        # Specific importe: There does not exist a specific match in importe_nominal.
        elif not specific_day_results and importe_nominal:
            lower_value = cassandra.best_tasa(table, pais_destino, divisa,
                                              num_agente=num_agente,
                                              competidor=competidor,
                                              day=day,
                                              month=month,
                                              year=year,
                                              importe_nominal=importe_nominal,
                                              search='lower',
                                              mostrar=mostrar,)
            lower_value = float(lower_value[0][0]) if lower_value else None
            lower_value = importe_nominal - lower_value if lower_value and abs(lower_value) < 10000000.0 else None

            upper_value = cassandra.best_tasa(table, pais_destino, divisa,
                                              num_agente=num_agente,
                                              competidor=competidor,
                                              day=day,
                                              month=month,
                                              year=year,
                                              importe_nominal=importe_nominal,
                                              search='upper',
                                              mostrar=mostrar,)
            upper_value = float(upper_value[0][0]) if upper_value else None
            upper_value = upper_value + importe_nominal if upper_value and upper_value < 10000000.0 else None

            if lower_value:
                lower_tasa_results = cassandra.best_tasa(table, pais_destino, divisa,
                                                         num_agente=num_agente,
                                                         competidor=competidor,
                                                         day=day,
                                                         month=month,
                                                         year=year,
                                                         importe_nominal=lower_value,
                                                         mostrar=mostrar, )
            if upper_value:
                upper_tasa_results = cassandra.best_tasa(table, pais_destino, divisa,
                                num_agente=num_agente,
                                competidor=competidor,
                                day=day,
                                month=month,
                                year=year,
                                importe_nominal=upper_value,
                                mostrar=mostrar,)

        results.append(specific_day_results)

    # Join and sort information for every day
    results_dict = dict()
    for cont, days in enumerate(results):
        for d in days:
            results_dict[str(cont)+str(d)] = d
    ordered_results = sorted_candidates = OrderedDict(
        sorted(results_dict.items(), key=lambda t: float(t[1][importe_destino_id]), reverse=True))
    results = list(ordered_results.values())[:last_mostrar]

    return render_template('query3_result.html', **locals())

if __name__ == "__main__":
    app.run(host='127.0.0.2', port=80)