package monty.captadorprecios;


import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

import org.joda.time.Days;
import org.joda.time.DurationFieldType;
import org.joda.time.LocalDate;

import ch.hsr.geohash.*;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;

public class CassandraLogic {

	/*
	 * Cassandra contact point address.
	 */
	private static String contactPoint = "127.0.0.1";
	/*
	 * Definition of the common attributes of the tables.
	 * If an attribute is particular of a table (i.e. geohash), define it in the particular table.
	 */
	private static String attributes = "pais_destino text, " 
			+ "ciudad text, "  
			+ "divisa text, "
			+ "year int, "
			+ "month int, " 
			+ "day int, "  
			+ "importe_destino double, "  
			+ "competidor text, "  
			+ "comision double, "  
			+ "tasa_cambio double, "  
			+ "timestamp double, "  
			+ "lat double, "  
			+ "lon double, "  
			+ "num_agente int, "  
			+ "importe_nominal double, ";
		
	/*
	 * This variable is used to rank the rows by the best "tasa" ("tasa" = tasa_cambio - comision).
	 */
	private static final int importeDestinoIdDefault = 6;

	/*
	 * Definition of all the tables and their queries.
	 * We will use this map to create all the tables at the same time.
	 * 
	 * @return map with all the tables and queries.
	 */
	private static HashMap<String, String> defineTables(){
		HashMap<String, String> tables = new HashMap<String, String>();

		tables.put("pais_query", pais());
		tables.put("pais_competidor_query",  paisCompetidor());
		tables.put("pais_importe_nominal_query",  paisImporteNominal());
		tables.put("pais_competidor_importe_nominal_query",  paisCompetidorImporteNominal());

		tables.put("ciudad_query",  ciudad());
		tables.put("ciudad_competidor_query",  ciudadCompetidor());
		tables.put("ciudad_importe_nominal_query",  ciudadImporteNominal());
		tables.put("ciudad_competidor_importe_nominal_query",  ciudadCompetidorImporteNominal());

		tables.put("agente_query",  agente());
		tables.put("agente_competidor_query",  agenteCompetidor());
		tables.put("agente_importe_nominal_query",  agenteImporteNominal());
		tables.put("agente_competidor_importe_nominal_query",  agenteCompetidorImporteNominal());

		tables.put("geohash_scheme",  geohash_scheme());
		tables.put("geohash_competidor_scheme",  geohash_competidor_scheme());
		tables.put("geohash_query",  geohash());
		tables.put("geohash_competidor_query",  geohashCompetidor());
		tables.put("geohash_importe_nominal_query",  geohashImporteNominal());
		tables.put("geohash_competidor_importe_nominal_query",  geohashCompetidorImporteNominal());
		return tables;
	}

	/*
	 * Key structure of the table:
	 * pais_destino, divisa, year, month, day, importe_destino, timestamp
	 * 
	 * Purpose: general searches by pais_destino.
	 */
	private static String pais(){
		String tableName = "pais_query";
		return "CREATE TABLE " + tableName + "(" 
		+  attributes 
		+ "PRIMARY KEY (pais_destino, divisa, year, month, day, importe_destino, timestamp) )" 
		+ "WITH CLUSTERING ORDER BY (divisa DESC, year DESC, month DESC, day DESC, importe_destino DESC, timestamp DESC);";
	}

	/*
	 * Key structure of the table:
	 * pais_destino, divisa, competidor, year, month, day, importe_destino, timestamp
	 * 
	 * Purpose: general searches by pais_destino for a particular competidor.
	 */
	private static String paisCompetidor(){
		String tableName = "pais_competidor_query";
		return "CREATE TABLE " + tableName + "("
		+  attributes 
		+ "PRIMARY KEY (pais_destino, divisa, competidor, year, month, day, importe_destino, timestamp) )"
		+ "WITH CLUSTERING ORDER BY (divisa DESC, competidor DESC, year DESC, month DESC, day DESC, importe_destino DESC, timestamp DESC);";
	}

	/*
	 * Key structure of the table:
	 * pais_destino, divisa, year, month, day, importe_nominal, importe_destino, timestamp
	 * 
	 * Purpose: searches by pais_destino for a particular importe_nominal.
	 */
	private static String paisImporteNominal(){
		String tableName = "pais_importe_nominal_query";
		return "CREATE TABLE " + tableName + "(" 
		+  attributes 
		+ "PRIMARY KEY (pais_destino, divisa, year, month, day, importe_nominal, importe_destino, timestamp) )"
		+ "WITH CLUSTERING ORDER BY (divisa DESC, year DESC, month DESC, day DESC, importe_nominal DESC, importe_destino DESC, timestamp DESC);";
	}

	/*
	 * Key structure of the table:
	 * pais_destino, divisa, competidor, year, month, day, importe_nominal, importe_destino, timestamp
	 * 
	 * Purpose: searches by pais_destino for a particular importe_nominal and a particular competidor.
	 */
	private static String paisCompetidorImporteNominal(){
		String tableName = "pais_competidor_importe_nominal_query";
		return "CREATE TABLE " + tableName + "(" 
		+  attributes 
		+ "PRIMARY KEY (pais_destino, divisa, competidor, year, month, day, importe_nominal, importe_destino, timestamp) )" 
		+ "WITH CLUSTERING ORDER BY (divisa DESC, competidor DESC, year DESC, month DESC, day DESC, importe_nominal DESC, importe_destino DESC, timestamp DESC);";
	}

	/*
	 * Key structure of the table:
	 * pais_destino, ciudad, divisa, year, month, day, importe_destino, timestamp
	 * 
	 * Purpose: general searches by pais_destino and ciudad 
	 */
	private static String ciudad(){
		String tableName = "ciudad_query";
		return "CREATE TABLE " + tableName + "(" 
		+  attributes 
		+ "PRIMARY KEY (pais_destino, ciudad, divisa, year, month, day, importe_destino, timestamp) )" 
		+ "WITH CLUSTERING ORDER BY (ciudad DESC, divisa DESC, year DESC, month DESC, day DESC, importe_destino DESC, timestamp DESC);";
	}

	/*
	 * Key structure of the table:
	 * pais_destino, ciudad, competidor, divisa, year, month, day, importe_destino, timestamp
	 * 
	 * Purpose: general searches by pais_destino and ciudad for a particular competidor.
	 */
	private static String ciudadCompetidor(){
		String tableName = "ciudad_competidor_query";
		return "CREATE TABLE " + tableName + "("
		+  attributes 
		+ "PRIMARY KEY (pais_destino, ciudad, divisa, competidor, year, month, day, importe_destino, timestamp) )"
		+ "WITH CLUSTERING ORDER BY (ciudad DESC, divisa DESC, competidor DESC, year DESC, month DESC, day DESC, importe_destino DESC, timestamp DESC);";
	}

	/*
	 * Key structure of the table:
	 * pais_destino, ciudad, divisa, year, month, day, importe_nominal, importe_destino, timestamp
	 * 
	 * Purpose: general searches by pais_destino and ciudad for a particular importe_nominal
	 */
	private static String ciudadImporteNominal(){
		String tableName = "ciudad_importe_nominal_query";
		return "CREATE TABLE " + tableName + "(" 
		+  attributes 
		+ "PRIMARY KEY (pais_destino, ciudad, divisa, year, month, day, importe_nominal, importe_destino, timestamp) )"
		+ "WITH CLUSTERING ORDER BY (ciudad DESC, divisa DESC, year DESC, month DESC, day DESC, importe_nominal DESC, importe_destino DESC, timestamp DESC);";
	}

	/*
	 * Key structure of the table:
	 * pais_destino, ciudad, divisa, competidor, year, month, day, importe_nominal, importe_destino, timestamp
	 * 
	 * Purpose: general searches by pais_destino and ciudad for a particular importe_nominal and particular competidor.
	 */
	private static String ciudadCompetidorImporteNominal(){
		String tableName = "ciudad_competidor_importe_nominal_query";
		return "CREATE TABLE " + tableName + "(" 
		+  attributes 
		+ "PRIMARY KEY (pais_destino, ciudad, divisa, competidor, year, month, day, importe_nominal, importe_destino, timestamp) )" 
		+ "WITH CLUSTERING ORDER BY (ciudad DESC, divisa DESC, competidor DESC, year DESC, month DESC, day DESC, importe_nominal DESC, importe_destino DESC, timestamp DESC);";
	}

	/*
	 * Key structure of the table:
	 * pais_destino, num_agente, divisa, year, month, day, importe_destino, timestamp
	 * 
	 * Purpose: general searches by pais_destino and num_agente 
	 */
	private static String agente(){
		String tableName = "agente_query";
		return "CREATE TABLE " + tableName + "(" 
		+  attributes 
		+ "PRIMARY KEY (pais_destino, num_agente, divisa, year, month, day, importe_destino, timestamp) )" 
		+ "WITH CLUSTERING ORDER BY (num_agente DESC, divisa DESC, year DESC, month DESC, day DESC, importe_destino DESC, timestamp DESC);";
	}

	/*
	 * Key structure of the table:
	 * pais_destino, num_agente, divisa, competidor, year, month, day, importe_destino, timestamp
	 * 
	 * Purpose: general searches by pais_destino and num_agente for a particular competidor
	 */
	private static String agenteCompetidor(){
		String tableName = "agente_competidor_query";
		return "CREATE TABLE " + tableName + "("
		+  attributes 
		+ "PRIMARY KEY (pais_destino, num_agente, divisa, competidor, year, month, day, importe_destino, timestamp) )"
		+ "WITH CLUSTERING ORDER BY (num_agente DESC, divisa DESC, competidor DESC, year DESC, month DESC, day DESC, importe_destino DESC, timestamp DESC);";
	}

	/*
	 * Key structure of the table:
	 * pais_destino, num_agente, divisa, year, month, day, importe_nominal, importe_destino, timestamp
	 * 
	 * Purpose: general searches by pais_destino and num_agente for a particular importe_nominal 
	 */
	private static String agenteImporteNominal(){
		String tableName = "agente_importe_nominal_query";
		return "CREATE TABLE " + tableName + "(" 
		+  attributes 
		+ "PRIMARY KEY (pais_destino, num_agente, divisa, year, month, day, importe_nominal, importe_destino, timestamp) )"
		+ "WITH CLUSTERING ORDER BY (num_agente DESC, divisa DESC, year DESC, month DESC, day DESC, importe_nominal DESC, importe_destino DESC, timestamp DESC);";
	}

	/*
	 * Key structure of the table:
	 * pais_destino, num_agente, divisa, competidor, year, month, day, importe_nominal, importe_destino, timestamp
	 * 
	 * Purpose: general searches by pais_destino and num_agente for a particular importe_nominal and particular competidor.
	 */
	private static String agenteCompetidorImporteNominal(){
		String tableName = "agente_competidor_importe_nominal_query";
		return "CREATE TABLE " + tableName + "(" 
		+  attributes 
		+ "PRIMARY KEY (pais_destino, num_agente, divisa, competidor, year, month, day, importe_nominal, importe_destino, timestamp) )" 
		+ "WITH CLUSTERING ORDER BY (num_agente DESC, divisa DESC, competidor DESC, year DESC, month DESC, day DESC, importe_nominal DESC, importe_destino DESC, timestamp DESC);";
	}

	/*
	 * Key structure of the table:
	 * pais_destino, divisa, geohash, competidor
	 * 
	 * Purpose: store unique geopositions for a given pais_destino and divisa. We store its geohash (string combination of lat and lon).
	 */
	private static String geohash_scheme(){
		String tableName = "geohash_scheme";
		return "CREATE TABLE " + tableName + "(" 
		+  attributes 
		+ "geohash text, "
		+ "PRIMARY KEY (pais_destino, divisa, geohash, competidor) )" 
		+ "WITH CLUSTERING ORDER BY (divisa DESC, geohash DESC, competidor DESC);";
	}

	/*
	 * Key structure of the table:
	 * pais_destino, divisa, competidor, geohash
	 * 
	 * Purpose: store unique geopositions for a given pais_destino, divisa and competidor. We store its geohash (string combination of lat and lon).
	 */	
	private static String geohash_competidor_scheme(){
		String tableName = "geohash_competidor_scheme";
		return "CREATE TABLE " + tableName + "(" 
		+  attributes 
		+ "geohash text, "
		+ "PRIMARY KEY (pais_destino, divisa, competidor, geohash) )" 
		+ "WITH CLUSTERING ORDER BY (divisa DESC, competidor DESC, geohash DESC);";
	}

	/*
	 * Key structure of the table:
	 * pais_destino, geohash, divisa, year, month, day, importe_destino, timestamp
	 * 
	 * Purpose: general searches by pais_destino and geohash 
	 */
	private static String geohash(){
		String tableName = "geohash_query";
		return "CREATE TABLE " + tableName + "(" 
		+  attributes 
		+ "geohash text, "
		+ "PRIMARY KEY (pais_destino, geohash, divisa, year, month, day, importe_destino, timestamp) )" 
		+ "WITH CLUSTERING ORDER BY (geohash DESC, divisa DESC, year DESC, month DESC, day DESC, importe_destino DESC, timestamp DESC);";
	}

	/*
	 * Key structure of the table:
	 * pais_destino, geohash, divisa, competidor, year, month, day, importe_destino, timestamp
	 * 
	 * Purpose: general searches by pais_destino and geohash for a particular competidor.
	 */
	private static String geohashCompetidor(){
		String tableName = "geohash_competidor_query";
		return "CREATE TABLE " + tableName + "("
		+  attributes 
		+ "geohash text, "
		+ "PRIMARY KEY (pais_destino, geohash, divisa, competidor, year, month, day, importe_destino, timestamp) )"
		+ "WITH CLUSTERING ORDER BY (geohash DESC, divisa DESC, competidor DESC, year DESC, month DESC, day DESC, importe_destino DESC, timestamp DESC);";
	}

	/*
	 * Key structure of the table:
	 * pais_destino, geohash, divisa, year, month, day, importe_nominal, importe_destino, timestamp
	 * 
	 * Purpose: general searches by pais_destino and geohash for a particular importe_nominal.
	 */
	private static String geohashImporteNominal(){
		String tableName = "geohash_importe_nominal_query";
		return "CREATE TABLE " + tableName + "(" 
		+  attributes 
		+ "geohash text, "
		+ "PRIMARY KEY (pais_destino, geohash, divisa, year, month, day, importe_nominal, importe_destino, timestamp) )"
		+ "WITH CLUSTERING ORDER BY (geohash DESC, divisa DESC, year DESC, month DESC, day DESC, importe_nominal DESC, importe_destino DESC, timestamp DESC);";
	}

	/*
	 * Key structure of the table:
	 * pais_destino, geohash, divisa, competidor, year, month, day, importe_nominal, importe_destino, timestamp
	 * 
	 * Purpose: general searches by pais_destino and geohash for a particular importe_nominal and a particular competidor.
	 */
	private static String geohashCompetidorImporteNominal(){
		String tableName = "geohash_competidor_importe_nominal_query";
		return "CREATE TABLE " + tableName + "(" 
		+  attributes 
		+ "geohash text, "
		+ "PRIMARY KEY (pais_destino, geohash, divisa, competidor, year, month, day, importe_nominal, importe_destino, timestamp) )" 
		+ "WITH CLUSTERING ORDER BY (geohash DESC, divisa DESC, competidor DESC, year DESC, month DESC, day DESC, importe_nominal DESC, importe_destino DESC, timestamp DESC);";
	}

	/*
	 * Method to create a new connection when the keyspace hasn't been defined yet.
	 * 
	 * @return session object.
	 */
	private static Session createConnection(){
		Cluster cluster = Cluster.builder()
				.addContactPoint(contactPoint)
				.build();
		return cluster.connect();
	}

	/*
	 * Method to create a connection given a keyspace.
	 * 
	 * @return session object.
	 */
	private static Session connect(){
		Cluster cluster = Cluster.builder()
				.addContactPoint(contactPoint)
				.build();
		return cluster.connect("precios_competencia");

	}

	/* Drop and create new key space.
	 * 
	 * First, it tries to drop the key space and then it creates the key space. 
	 * If the drop fails, it creates directly the key space.
	 * 
	 *  Default parameters: 
	 * 	+ replication strategy: SimpleStrategy
	 * 	+ replication factor: 3
	 */
	public static void dropAndCreateKeySpace(){
		String keySpaceName = "precios_competencia";
		Session ses = createConnection();

		String createKeySpaceQuery = "CREATE KEYSPACE "+ keySpaceName
				+ " WITH replication = {'class': 'SimpleStrategy',"
				+ "'replication_factor' : 3}";	
		try{ 
			ses.execute("Drop KEYSPACE "+keySpaceName);
			ses.execute(createKeySpaceQuery);
		}	    
		catch(InvalidQueryException e){ // The drop fails.
			ses.execute(createKeySpaceQuery);
		}finally{		
			// We define the user defined functions (UDF).
			ses.execute(nearestLowerImporteFunction());
			ses.execute(nearestUpperImporteFunction());
		}
	}

	/* Drop and create tables.
	 * 
	 * First it tries to drop the table and then it creates the table.
	 * If the drop fails, it creates directly the table.
	 * 
	 */
	public static void dropAndCreateTables(){
		Session ses = connect();

		// Map with all the tables names and queries.
		HashMap<String, String> tables = defineTables();
		for(String tableName: tables.keySet()){
			try{
				ses.execute("Drop TABLE "+tableName);
				ses.execute(tables.get(tableName));
			}
			catch(InvalidQueryException e){ // The drop fails.
				ses.execute(tables.get(tableName));;
			}
		}
	}

	/* Insert data into all tables.
	 * 
	 * Note: we omit the data insertion in the geohash tables if lat and lon are not defined.
	 * 
	 */
	public static void insertIntoAllTables(String paisDestino, String divisa, String competidor,
			String ciudad, Integer numAgente, 
			Double lat, Double lon,
			Double tasaCambio, Double comision, Integer importeNominal, Double importeDestino,
			Double timestamp, Integer year, Integer month, Integer day){
		
		Session ses = connect();

		// Map with all the tables names and queries.
		HashMap<String, String> tables = defineTables();		
		boolean coordsDefined = lat != null && lon != null;

		for(String tableName: tables.keySet()){
			String geohash = null;
			if(tableName.startsWith("geohash")){
				if(coordsDefined){ // if we have lat and lon, insert. Else, pass.
					geohash = GeoHash.geoHashStringWithCharacterPrecision(lat, lon, 12); //Generate geohash			
					insertData(ses, tableName, paisDestino, divisa, competidor, ciudad, numAgente, lat, lon, tasaCambio, comision, importeNominal, importeDestino,
							timestamp, year, month, day, geohash);					
				}
			}
			else{
				insertData(ses, tableName,  paisDestino, divisa, competidor, ciudad, numAgente, lat, lon, tasaCambio, comision, importeNominal, importeDestino,
						timestamp, year, month, day, geohash);
			}
		}
	}

	/* Insert a row in the table.
	 */
	private static void insertData(Session ses, String tableName, String paisDestino, String divisa, String competidor,
			String ciudad, Integer numAgente, 
			Double lat, Double lon,
			Double tasaCambio, Double comision, Integer importeNominal, Double importeDestino,
			Double timestamp, Integer year, Integer month, Integer day, String geohash){
		
		boolean insertFlag = true;
		if(tableName.endsWith("scheme")){ // Only insert information in scheme tables if it does not already exist (unique geopositions).
			String checkDataQuery = "SELECT geohash FROM "+tableName+" WHERE ";
			checkDataQuery += "pais_destino='"+paisDestino+"'"
					+" AND divisa='"+divisa+"'"
					+" AND geohash='"+geohash+"'"
					+" AND competidor='"+competidor+"'";

			insertFlag =  ses.execute(checkDataQuery).one() == null? true: false;
		}

		if(insertFlag){
			String query = "INSERT INTO "+tableName+"(";
			String columnNamePart= "pais_destino, divisa, competidor, ciudad, num_agente, tasa_cambio, comision, importe_nominal, importe_destino, ";
			columnNamePart += "timestamp, year, month, day";
			
			String columnValuePart = " VALUES(";
			columnValuePart += "'"+paisDestino+"', "+"'"+divisa+"', "+"'"+competidor+"', "+"'"+ciudad+"', "+String.valueOf(numAgente)+", "+String.valueOf(tasaCambio)+", ";
			columnValuePart += String.valueOf(comision)+", "+String.valueOf(importeNominal)+", "+String.valueOf(importeDestino)+", "+String.valueOf(timestamp)+", ";
			columnValuePart += String.valueOf(year)+", "+String.valueOf(month)+", "+String.valueOf(day);
			
			if(lat != null){
				columnNamePart += ", lat";
				columnValuePart += ", "+String.valueOf(lat);
				if(lon != null){
					columnNamePart += ", lon";
					columnValuePart += ", "+String.valueOf(lon);
				}
			}
			
			if(geohash != null){
				columnNamePart += ", geohash";
				columnValuePart += ", '"+String.valueOf(geohash)+"'";
			}
			
			columnNamePart += ")";
			columnValuePart += ");";
			
			query += columnNamePart + columnValuePart;
			ses.execute(query);
		}
	}

	///////////////////////////////////////////////////// QUERIES ///////////////////////////////////////////////////////////

	/*
	 * Hay cuatro tipo de queries dependiendo en que queremos buscar:
	 * 1. Pais_destino 	
	 * 2. Ciudad
	 * 3. Num_agente
	 * 4. Geolocalizacion
	 * 
	 * Por otro lado, para cada uno de los casos anteriores, existen las siguientes queries:
	 * a. Buscar por fecha concreta.
	 * b. Buscar por fecha concreta e importe nominal exacto.
	 * c. Buscar por fecha concreta e importe nominal más cercano por arriba y por abajo (útil si el caso b) da null).
	 * d. Buscar por fecha concreta y un rango de importes.
	 * 
	 * e. Buscar por rango de fechas.
	 * f. Buscar por rango de fechas e importe nominal exacto.
	 * g. Buscar por rango de fechas e importe nominal más cercano por arriba y por abajo (úlil si el caso f) da null).
	 * h. Buscar por rango de fechas y rango de importes.
	 * 
	 * Además, para cada una de las querie de a) hasta h) existen dos casos diferentes: para todos los competidores, o para un competidor en especifico.
	 */


	public static List<Row> seleccionarMejoresTasas(String paisDestino, String divisa, String competidor,
			String ciudad, Integer numAgente, Double lat, Double lon, Integer distancia, 
			Integer importeNominal, Integer minRangeImporteNominal, Integer maxRangeImporteNominal,
			Integer year, Integer month, Integer day, Integer maxYear, Integer maxMonth, Integer maxDay, String searchFlag)
			{

		Session ses = connect();

		if(lat != null && lon != null && distancia != null){ // Geolocalizaciones caso.
			return bestTasasGeolocalizacion(ses, paisDestino, divisa, competidor,
					ciudad, numAgente, lat, lon, distancia, 
					importeNominal, minRangeImporteNominal, maxRangeImporteNominal,
					year, month, day, maxYear, maxMonth, maxDay, searchFlag);
		}
		else if(paisDestino != null || ciudad != null || numAgente != null){
			return bestTasas(ses, paisDestino, divisa, competidor,
					ciudad, numAgente, null,  
					importeNominal, minRangeImporteNominal, maxRangeImporteNominal,
					year, month, day, maxYear, maxMonth, maxDay, null, null,
					searchFlag);
		}
		else{
			return null;
		}
			}
	
	/*
	 * Get geolocalizaciones within a distance for a given point (lat, lon).
	 */
	private static List<Row> getGeolocalizaciones(Session ses, String paisDestino, Double lat, Double lon, Integer distancia, String divisa, String competidor){
		GeoLocation location = GeoLocation.fromDegrees(lat, lon);
		GeoLocation[] boundingCoordinates =
				location.boundingCoordinates(distancia);

		Double minLat = boundingCoordinates[0].getLatitudeInDegrees();
		Double minLon = boundingCoordinates[0].getLongitudeInDegrees();
		Double maxLat = boundingCoordinates[1].getLatitudeInDegrees();
		Double maxLon = boundingCoordinates[1].getLongitudeInDegrees();

		String maxGeohash = GeoHash.geoHashStringWithCharacterPrecision(maxLat, maxLon, 12);
		String minGeohash = GeoHash.geoHashStringWithCharacterPrecision(minLat, minLon, 12);

		String tableName = "geohash_";
		if(competidor != null){
			tableName += "competidor_";
		}
		tableName += "scheme";
		
		return bestTasaGivenDate(ses, tableName, paisDestino, divisa, competidor,
				null, null, null,
				null, null, null,
				null, null, null, null, null, null,
				minGeohash, maxGeohash,
				null);
	}

	/*
	 * Get best tasas for a set of given geolocalizaciones.
	 */
	private static List<Row> bestTasasGeolocalizacion(Session ses, String paisDestino, String divisa, String competidor,
			String ciudad, Integer numAgente, Double lat, Double lon, Integer distancia, 
			Integer importeNominal, Integer minRangeImporteNominal, Integer maxRangeImporteNominal,
			Integer year, Integer month, Integer day, Integer maxYear, Integer maxMonth, Integer maxDay, String searchFlag){

		List<Row> geolocalizaciones = getGeolocalizaciones(ses, paisDestino, lat, lon, distancia, divisa, competidor);

		List<Row> toReturn = new ArrayList<Row>();
		for(int i=0; i<geolocalizaciones.size(); i++){
			String geohash = geolocalizaciones.get(i).getString(0);
			toReturn.addAll(bestTasas(ses, paisDestino, divisa, competidor,
					ciudad, numAgente, geohash,
					importeNominal, minRangeImporteNominal, maxRangeImporteNominal,
					year, month, day, maxYear, maxMonth, maxDay,
					null, null,
					searchFlag));
		}

		int parameterVariation = 0;
		if(competidor != null){
			parameterVariation += 1;
		}

		final int importeDestinoId = importeDestinoIdDefault+parameterVariation;
		Collections.sort(toReturn, new Comparator<Row>() {
			@Override
			public int compare(Row o1, Row o2) {
				if(o1.getDouble(importeDestinoId)>o2.getDouble(importeDestinoId)) return -1;
				else if(o1.getDouble(importeDestinoId)<o2.getDouble(importeDestinoId)) return 1;
				return 0;
			}
		});
		return toReturn;
	}


	/*
	 * Match all the tasas given two periods of time. 
	 * 
	 * Note 1: If you are only interested in a single date, it is allowed (start=end date).
	 * Note 2: If you search an exact importe_nominal, and it does not exists, you can look for the upper and lower tasas.
	 * 
	 * @return list of rows with the matches.
	 */
	private static List<Row> bestTasas(Session ses, String paisDestino, String divisa, String competidor,
			String ciudad, Integer numAgente, String geohash,
			Integer importeNominal, Integer minRangeImporteNominal, Integer maxRangeImporteNominal,
			Integer year, Integer month, Integer day, Integer maxYear, Integer maxMonth, Integer maxDay,
			String minGeohash, String maxGeohash,
			String searchFlag){

		String tableName = "";
		String keyField = "";
		String competidorField = "";
		int parameterVariation = 0;
		boolean pleaseSort = true;

		if(ciudad != null){
			keyField = "ciudad_";
		}
		else if(numAgente != null){
			keyField = "agente_";
		}
		else if(geohash != null){
			keyField = "geohash_";
			pleaseSort = false;
		}
		else{
			keyField = "pais_";
			parameterVariation -= 1;
		}

		if(competidor != null){
			competidorField = "competidor_";
			parameterVariation += 1;
		}

		tableName = keyField + competidorField;
		if(importeNominal != null || (minRangeImporteNominal != null && maxRangeImporteNominal != null)){
			tableName += "importe_nominal_";
			parameterVariation += 1;
		}
		tableName += "query";		

		// Start date in string format.
		String s = String.valueOf(year)+"-"+String.valueOf(month)+"-"+String.valueOf(day);

		// End date in string format.
		String e = s;

		if(maxYear != null && maxMonth != null && maxDay != null){// If end date is defined, change end date.
			e = String.valueOf(maxYear)+"-"+String.valueOf(maxMonth)+"-"+String.valueOf(maxDay);
		}

		// Calculate number of days between start date and end date.
		LocalDate startDate = LocalDate.parse(s);
		LocalDate endDate = LocalDate.parse(e);
		int days = Days.daysBetween(startDate, endDate).getDays();


		ArrayList<Row> toReturn = new ArrayList<Row>();
		for (int i=0; i <= days; i++) {
			LocalDate d = startDate.withFieldAdded(DurationFieldType.days(), i);
			Integer year2 = Integer.valueOf(d.year().getAsString());
			Integer month2 = Integer.valueOf(d.monthOfYear().getAsString());
			Integer day2 = Integer.valueOf(d.dayOfMonth().getAsString());	


			toReturn.addAll(bestTasaGivenDate(ses, tableName, paisDestino, divisa, competidor,
					ciudad, numAgente, geohash,
					importeNominal, minRangeImporteNominal, maxRangeImporteNominal,
					year2, month2, day2, maxYear, maxMonth, maxDay,
					minGeohash, maxGeohash,
					searchFlag));
		} 
		if(pleaseSort){
			final int importeDestinoId = importeDestinoIdDefault+parameterVariation;
			Collections.sort(toReturn, new Comparator<Row>() {
				@Override
				public int compare(Row o1, Row o2) {
					if(o1.getDouble(importeDestinoId)>o2.getDouble(importeDestinoId)) return -1;
					else if(o1.getDouble(importeDestinoId)<o2.getDouble(importeDestinoId)) return 1;
					return 0;
				}
			});
		}
		return toReturn;
	}

	/*
	 * Match the tasas for a given date.
	 * 
	 * @return list of rows with the matches.
	 */
	private static List<Row> bestTasaGivenDate(Session ses, String tableName, String paisDestino, String divisa, String competidor,
			String ciudad, Integer numAgente, String geohash,
			Integer importeNominal, Integer minRangeImporteNominal, Integer maxRangeImporteNominal,
			Integer year, Integer month, Integer day, Integer maxYear, Integer maxMonth, Integer maxDay,
			String minGeohash, String maxGeohash,
			String searchFlag){

		String sel = "* ";
		if(searchFlag != null && searchFlag.equals("lower")){
			sel = "min(nearest_lower_importe("+String.valueOf(importeNominal)+", importe_nominal))";
		}
		else if(searchFlag != null && searchFlag.equals("upper")){
			sel = "min(nearest_upper_importe("+String.valueOf(importeNominal)+", importe_nominal))";
		}
		else if(maxGeohash != null && minGeohash != null){
			sel = "geohash ";
		}

		String query = "SELECT "+sel+"FROM "+tableName+" ";
		query += "WHERE pais_destino='"+paisDestino+"' ";

		if(ciudad != null){
			query += "AND ciudad='"+ciudad+"' ";
		}
		else if(numAgente != null){
			query += "AND num_agente="+String.valueOf(numAgente)+" ";
		}
		else if(geohash != null){
			query += "AND geohash='"+geohash+"' ";
		}

		query += "AND divisa='"+divisa+"' ";

		if(competidor != null){
			query += "AND competidor='"+competidor+"' ";
		}

		if(year != null & month != null & day != null){
			query += "AND year="+String.valueOf(year)+" AND month="+String.valueOf(month)+" AND day="+String.valueOf(day)+" ";
		}

		if(importeNominal != null && searchFlag == null){
			query += "AND importe_nominal="+String.valueOf(importeNominal)+" ";
		}

		if(minRangeImporteNominal != null && maxRangeImporteNominal != null){
			query += "AND importe_nominal >= "+String.valueOf(minRangeImporteNominal)+" AND importe_nominal <= "+String.valueOf(maxRangeImporteNominal)+" ";
		}

		if(maxGeohash != null && minGeohash != null){
			query += "AND geohash <= '"+maxGeohash+"' AND geohash >= '"+minGeohash+"' ";
		}

		ResultSet results = ses.execute(query);
		return results.all();
	}

	private static String nearestLowerImporteFunction(){
		return "CREATE FUNCTION precios_competencia.nearest_lower_importe(importe_user double, importe_nominal double) "
				+ "RETURNS NULL ON NULL INPUT "
				+ "RETURNS double "
				+ "LANGUAGE java "
				+ "AS ' "
				+ "if(importe_nominal > importe_user){ "
				+ "	return 10000000.0;"
				+ "} "
				+ "else{ "
				+ "	return importe_user-importe_nominal; "
				+ "}'";
	}

	private static String nearestUpperImporteFunction(){
		return "CREATE FUNCTION precios_competencia.nearest_upper_importe(importe_user double, importe_nominal double) "
				+ "RETURNS NULL ON NULL INPUT "
				+ "RETURNS double "
				+ "LANGUAGE java "
				+ "AS ' "
				+ "if(importe_nominal < importe_user){ "
				+ "	return 10000000.0;"
				+ "} "
				+ "else{ "
				+ "	return importe_nominal-importe_user; "
				+ "}'";
	}
}


