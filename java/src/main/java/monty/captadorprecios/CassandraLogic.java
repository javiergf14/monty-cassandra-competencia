package monty.captadorprecios;


import java.util.ArrayList;
import java.util.Arrays;
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
	 * Definition of the common attributes of the tables.
	 * If an attribute is particular of a table (i.e. geohash), define it in the particular table.
	 */
	private static String attributes = "pais_destino text, " 
			+ "ciudad text, "  
			+ "divisa text, "  
			+ "importe_destino double, "  
			+ "competidor text, "  
			+ "comision double, "  
			+ "tasa_cambio double, "  
			+ "timestamp double, "  
			+ "lat double, "  
			+ "lon double, "  
			+ "num_agente int, "  
			+ "importe_nominal double, "  
			+ "day int, "  
			+ "month int, "  
			+ "year int, ";

	private static final int importeDestinoIdDefault = 6;

	/*
	 * Definition of the String attributes.
	 * Note: everything that is not a string nor a integer will be a double.
	 */	
	private static List<String> stringAttributes = new ArrayList<>(
			Arrays.asList("pais_destino", "competidor", "divisa", "ciudad", "geohash"));

	/*
	 * Definition of the Integer attributes.
	 * Note: everything that is not a string nor a integer will be a double.
	 */
	private static List<String> intAttributes = new ArrayList<>(
			Arrays.asList("num_agente", "day", "month", "year"));


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
		Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").withPort(9042).build();
		return cluster.connect();
	}

	/*
	 * Method to create a connection given a keyspace.
	 * 
	 * @return session object.
	 */
	private static Session connect(){
		Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").withPort(9042).build();
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
	 * @param data, map with column names as keys and column values as values. 
	 * 
	 */
	public static void insertIntoAllTables(HashMap<String, String> data){
		Session ses = connect();

		// Map with all the tables names and queries.
		HashMap<String, String> tables = defineTables();		
		boolean coordsDefined = data.containsKey("lat") && data.containsKey("lon");

		for(String tableName: tables.keySet()){			
			if(tableName.startsWith("geohash")){
				if(coordsDefined){ // if we have lat and lon, insert. Else, pass.
					data.put("geohash", GeoHash.geoHashStringWithCharacterPrecision(
							Double.parseDouble(data.get("lat")), 
							Double.parseDouble(data.get("lon")), 
							12)); //Generate geohash			
					insertData(ses, tableName, data);
					data.remove("geohash");
				}
			}
			else{
				insertData(ses, tableName, data);
			}
		}
	}

	/* Insert a row in the table.
	 * 
	 * Notice we do not need to include all the column names nor columns values due to the flexible schema.
	 * The column values are integer by default, except if they are included in the stringList map.
	 * 
	 * @param tableName, name of the table.
	 * @param data, map with column names as keys and column values as values. 
	 */
	private static void insertData(Session ses, String tableName, HashMap<String, String> data){
		boolean insertFlag = true;
		if(tableName.endsWith("scheme")){ // Only insert information in geohash_X_scheme if it does not already exist (unique geopositions).
			String checkDataQuery = "SELECT geohash FROM "+tableName+" WHERE ";
			checkDataQuery += "pais_destino='"+data.get("pais_destino")+"'"
					+" AND divisa='"+data.get("divisa")+"'"
					+" AND geohash='"+data.get("geohash")+"'"
					+" AND competidor='"+data.get("competidor")+"'";

			insertFlag =  ses.execute(checkDataQuery).one() == null? true: false;
		}

		if(insertFlag){
			String query = "INSERT INTO "+tableName+"(";
			for(String columnName: data.keySet()){
				query += columnName+",";
			}
			query = query.substring(0, query.length()-1); // Remove last comma.
			query += ")";
			query += " VALUES(";

			for(String columnName: data.keySet()){
				if( stringAttributes.contains(columnName))
					query += "'"+data.get(columnName)+"'"+",";
				else if( intAttributes.contains(columnName))
					query += Integer.parseInt(data.get(columnName))+",";
				else // We consider the default variables as double.
					query += Double.parseDouble(data.get(columnName))+",";
			}
			query = query.substring(0, query.length()-1);
			query += ");";	
			ses.execute(query);
		}
	}

	/*
	 * Define default map parameters.
	 * 
	 * Everything is set to "" except the tableName, paisDestino, divisa and date.
	 * 
	 * @return map initialized to default values.
	 */
	private static HashMap<String, String> defineMapParameters(String tableName, String paisDestino, String divisa, String year, String month, String day){
		HashMap<String, String> mapParameters = new HashMap<String, String>();		
		mapParameters.put("tableName", tableName);
		mapParameters.put("paisDestino", paisDestino);
		mapParameters.put("divisa", divisa);
		mapParameters.put("year", year);
		mapParameters.put("month", month);
		mapParameters.put("day", day);

		mapParameters.put("numAgente", "");
		mapParameters.put("ciudad", "");
		mapParameters.put("competidor", "");
		mapParameters.put("searchFlag", "");
		mapParameters.put("importeNominal", "");
		mapParameters.put("minRangeImporteNominal", "");
		mapParameters.put("maxRangeImporteNominal", "");
		mapParameters.put("maxYear", "");
		mapParameters.put("maxMonth", "");
		mapParameters.put("maxDay", "");

		mapParameters.put("geohash", "");
		mapParameters.put("maxGeohash", "");
		mapParameters.put("minGeohash",  "");

		return mapParameters;
	}

	///////////////////////////////////////////////////// QUERIES ///////////////////////////////////////////////////////////

	/*
	 * Hay cuatro tipo de queries dependiendo en que queremos buscar:
	 * 1. Pais_destino (and anything else)
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

/*
	public static List<Row> seleccionarMejoresTasas(String paisDestino, String divisa, 
			String ciudad, String numAgente, String lat, String lon, String distancia, 
			String importeNominal, String minImporteNomnal, String maxImporteNominal,
			String year, String month, String day, String maxYear, String maxMonth, String maxDay)
			{
		
		
		
		
		return null;
	}*/
	/* * * * * * * * * * * * * * * * * * * * * * * * * * * * *
	 *
	 * Queries for PAIS DESTINO 
	 * 
	 */
	public static List<Row> seleccionarMejoresTasasPaisFechaConcreta(String paisDestino, String divisa, String year, String month, String day){
		final int importeDestinoId = importeDestinoIdDefault - 1;

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("pais_query", paisDestino, divisa, year, month, day);		

		ArrayList<Row> toReturn = (ArrayList<Row>) bestTasas(ses, mapParameters);
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

	public static List<Row> seleccionarMejoresTasasPaisFechaConcretaImporteNominalExacto(String paisDestino, String divisa, String year, 
			String month, String day, String importeNominal){

		final int importeDestinoId = importeDestinoIdDefault;
		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("pais_importe_nominal_query", paisDestino, divisa, year, month, day);	
		mapParameters.put("importeNominal", importeNominal);

		ArrayList<Row> toReturn = (ArrayList<Row>) bestTasas(ses, mapParameters);
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

	public static List<Row> seleccionarMejoresTasasPaisFechaConcretaImporteNominalExactoNoExisteLower(String paisDestino, String divisa, String year, 
			String month, String day, String importeNominal){
		
		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("pais_importe_nominal_query", paisDestino, divisa, year, month, day);	
		mapParameters.put("importeNominal", importeNominal);
		mapParameters.put("searchFlag", "lower");

		final int importeDestinoId = importeDestinoIdDefault;
		ArrayList<Row> toReturn = (ArrayList<Row>) bestTasas(ses, mapParameters);
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

	public static List<Row> seleccionarMejoresTasasPaisFechaConcretaImporteNominalExactoNoExisteUpper(String paisDestino, String divisa, String year, 
			String month, String day, String importeNominal){

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("pais_importe_nominal_query", paisDestino, divisa, year, month, day);		
		mapParameters.put("importeNominal", importeNominal);
		mapParameters.put("searchFlag", "upper");

		final int importeDestinoId = importeDestinoIdDefault;
		ArrayList<Row> toReturn = (ArrayList<Row>) bestTasas(ses, mapParameters);
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

	public static List<Row> seleccionarMejoresTasasPaisFechaConcretaRangoImporteNominal(String paisDestino, String divisa, String year, 
			String month, String day, String minRangeImporteNominal, String maxRangeImporteNominal){

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("pais_importe_nominal_query", paisDestino, divisa, year, month, day);	
		mapParameters.put("minRangeImporteNominal", minRangeImporteNominal);
		mapParameters.put("maxRangeImporteNominal", maxRangeImporteNominal);

		final int importeDestinoId = importeDestinoIdDefault;
		ArrayList<Row> toReturn = (ArrayList<Row>) bestTasas(ses, mapParameters);
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

	public static List<Row> seleccionarMejoresTasasPaisRangoFechas(String paisDestino, String divisa, 
			String minYear, String minMonth, String minDay,
			String maxYear, String maxMonth, String maxDay){
		

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("pais_query", paisDestino, divisa, minYear, minMonth, minDay);
		mapParameters.put("maxYear", maxYear);
		mapParameters.put("maxMonth", maxMonth);
		mapParameters.put("maxDay", maxDay);

		final int importeDestinoId = importeDestinoIdDefault - 1;
		ArrayList<Row> toReturn = (ArrayList<Row>) bestTasas(ses, mapParameters);
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

	public static List<Row> seleccionarMejoresTasasPaisRangoFechasImporteNominalExacto(String paisDestino, String divisa, 
			String minYear, String minMonth, String minDay,
			String maxYear, String maxMonth, String maxDay, 
			String importeNominal){

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("pais_importe_nominal_query", paisDestino, divisa, minYear, minMonth, minDay);
		mapParameters.put("maxYear", maxYear);
		mapParameters.put("maxMonth", maxMonth);
		mapParameters.put("maxDay", maxDay);
		mapParameters.put("importeNominal", importeNominal);

		final int importeDestinoId = importeDestinoIdDefault;
		ArrayList<Row> toReturn = (ArrayList<Row>) bestTasas(ses, mapParameters);
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

	public static List<Row> seleccionarMejoresTasasPaisRangoFechasImporteNominalExactoNoExisteLower(String paisDestino, String divisa, 
			String minYear, String minMonth, String minDay,
			String maxYear, String maxMonth, String maxDay, 
			String importeNominal){

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("pais_importe_nominal_query", paisDestino, divisa, minYear, minMonth, minDay);	
		mapParameters.put("maxYear", maxYear);
		mapParameters.put("maxMonth", maxMonth);
		mapParameters.put("maxDay", maxDay);
		mapParameters.put("importeNominal", importeNominal);
		mapParameters.put("searchFlag", "lower");

		final int importeDestinoId = importeDestinoIdDefault;
		ArrayList<Row> toReturn = (ArrayList<Row>) bestTasas(ses, mapParameters);
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

	public static List<Row> seleccionarMejoresTasasPaisRangoFechasImporteNominalExactoNoExisteUpper(String paisDestino, String divisa, 
			String minYear, String minMonth, String minDay,
			String maxYear, String maxMonth, String maxDay, 
			String importeNominal){

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("pais_importe_nominal_query", paisDestino, divisa, minYear, minMonth, minDay);
		mapParameters.put("maxYear", maxYear);
		mapParameters.put("maxMonth", maxMonth);
		mapParameters.put("maxDay", maxDay);
		mapParameters.put("importeNominal", importeNominal);
		mapParameters.put("searchFlag", "upper");

		final int importeDestinoId = importeDestinoIdDefault;
		ArrayList<Row> toReturn = (ArrayList<Row>) bestTasas(ses, mapParameters);
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

	public static List<Row> seleccionarMejoresTasasPaisRangoFechasRangoImporteNominal(String paisDestino, String divisa, 
			String minYear, String minMonth, String minDay,
			String maxYear, String maxMonth, String maxDay,
			String minRangeImporteNominal, String maxRangeImporteNominal){

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("pais_importe_nominal_query", paisDestino, divisa, minYear, minMonth, minDay);
		mapParameters.put("maxYear", maxYear);
		mapParameters.put("maxMonth", maxMonth);
		mapParameters.put("maxDay", maxDay);
		mapParameters.put("minRangeImporteNominal", minRangeImporteNominal);
		mapParameters.put("maxRangeImporteNominal", maxRangeImporteNominal);

		final int importeDestinoId = importeDestinoIdDefault;
		ArrayList<Row> toReturn = (ArrayList<Row>) bestTasas(ses, mapParameters);
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

	public static List<Row> seleccionarMejoresTasasPaisCompetidorFechaConcreta(String paisDestino, String divisa, String competidor, String year, String month, String day){

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("pais_competidor_query", paisDestino, divisa, year, month, day);
		mapParameters.put("competidor", competidor);


		final int importeDestinoId = importeDestinoIdDefault;
		ArrayList<Row> toReturn = (ArrayList<Row>) bestTasas(ses, mapParameters);
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

	public static List<Row> seleccionarMejoresTasasPaisCompetidorFechaConcretaImporteNominalExacto(String paisDestino, String divisa, String competidor, String year, 
			String month, String day, String importeNominal){
		

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("pais_competidor_importe_nominal_query", paisDestino, divisa, year, month, day);	
		mapParameters.put("competidor", competidor);
		mapParameters.put("importeNominal", importeNominal);

		final int importeDestinoId = importeDestinoIdDefault + 1;
		ArrayList<Row> toReturn = (ArrayList<Row>) bestTasas(ses, mapParameters);
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

	public static List<Row> seleccionarMejoresTasasPaisCompetidorFechaConcretaImporteNominalExactoNoExisteLower(String paisDestino, String divisa, String competidor, String year, 
			String month, String day, String importeNominal){
		final int importeDestinoId = importeDestinoIdDefault + 1;

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("pais_competidor_importe_nominal_query", paisDestino, divisa, year, month, day);	
		mapParameters.put("competidor", competidor);
		mapParameters.put("importeNominal", importeNominal);
		mapParameters.put("searchFlag", "lower");

		ArrayList<Row> toReturn = (ArrayList<Row>) bestTasas(ses, mapParameters);
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

	public static List<Row> seleccionarMejoresTasasPaisCompetidorFechaConcretaImporteNominalExactoNoExisteUpper(String paisDestino, String divisa, String competidor, String year, 
			String month, String day, String importeNominal){
		final int importeDestinoId = importeDestinoIdDefault + 1;

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("pais_competidor_importe_nominal_query", paisDestino, divisa, year, month, day);
		mapParameters.put("competidor", competidor);
		mapParameters.put("importeNominal", importeNominal);
		mapParameters.put("searchFlag", "upper");

		ArrayList<Row> toReturn = (ArrayList<Row>) bestTasas(ses, mapParameters);
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

	public static List<Row> seleccionarMejoresTasasPaisCompetidorFechaConcretaRangoImporteNominal(String paisDestino, String divisa, String competidor, String year, 
			String month, String day, String minRangeImporteNominal, String maxRangeImporteNominal){
		final int importeDestinoId = importeDestinoIdDefault + 1;

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("pais_competidor_importe_nominal_query", paisDestino, divisa, year, month, day);	
		mapParameters.put("competidor", competidor);
		mapParameters.put("minRangeImporteNominal", minRangeImporteNominal);
		mapParameters.put("maxRangeImporteNominal", maxRangeImporteNominal);

		ArrayList<Row> toReturn = (ArrayList<Row>) bestTasas(ses, mapParameters);
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

	public static List<Row> seleccionarMejoresTasasPaisCompetidorRangoFechas(String paisDestino, String divisa, String competidor, 
			String minYear, String minMonth, String minDay,
			String maxYear, String maxMonth, String maxDay){
		final int importeDestinoId = importeDestinoIdDefault + 1;

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("pais_competidor_importe_nominal_query", paisDestino, divisa, minYear, minMonth, minDay);
		mapParameters.put("competidor", competidor);
		mapParameters.put("maxYear", maxYear);
		mapParameters.put("maxMonth", maxMonth);
		mapParameters.put("maxDay", maxDay);

		ArrayList<Row> toReturn = (ArrayList<Row>) bestTasas(ses, mapParameters);
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

	public static List<Row> seleccionarMejoresTasasPaisCompetidorRangoFechasImporteNominalExacto(String paisDestino, String divisa, String competidor, 
			String minYear, String minMonth, String minDay,
			String maxYear, String maxMonth, String maxDay, 
			String importeNominal){
		final int importeDestinoId = importeDestinoIdDefault + 1;

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("pais_competidor_importe_nominal_query", paisDestino, divisa, minYear, minMonth, minDay);
		mapParameters.put("competidor", competidor);
		mapParameters.put("maxYear", maxYear);
		mapParameters.put("maxMonth", maxMonth);
		mapParameters.put("maxDay", maxDay);
		mapParameters.put("importeNominal", importeNominal);

		ArrayList<Row> toReturn = (ArrayList<Row>) bestTasas(ses, mapParameters);
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

	public static List<Row> seleccionarMejoresTasasPaisCompetidorRangoFechasImporteNominalExactoNoExisteLower(String paisDestino, String divisa, String competidor, 
			String minYear, String minMonth, String minDay,
			String maxYear, String maxMonth, String maxDay, 
			String importeNominal){
		final int importeDestinoId = importeDestinoIdDefault + 1;

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("pais_competidor_importe_nominal_query", paisDestino, divisa, minYear, minMonth, minDay);	
		mapParameters.put("competidor", competidor);
		mapParameters.put("maxYear", maxYear);
		mapParameters.put("maxMonth", maxMonth);
		mapParameters.put("maxDay", maxDay);
		mapParameters.put("importeNominal", importeNominal);
		mapParameters.put("searchFlag", "lower");

		ArrayList<Row> toReturn = (ArrayList<Row>) bestTasas(ses, mapParameters);
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

	public static List<Row> seleccionarMejoresTasasPaisCompetidorRangoFechasImporteNominalExactoNoExisteUpper(String paisDestino, String divisa, String competidor, 
			String minYear, String minMonth, String minDay,
			String maxYear, String maxMonth, String maxDay, 
			String importeNominal){
		final int importeDestinoId = importeDestinoIdDefault + 1;

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("pais_competidor_importe_nominal_query", paisDestino, divisa, minYear, minMonth, minDay);
		mapParameters.put("competidor", competidor);
		mapParameters.put("maxYear", maxYear);
		mapParameters.put("maxMonth", maxMonth);
		mapParameters.put("maxDay", maxDay);
		mapParameters.put("importeNominal", importeNominal);
		mapParameters.put("searchFlag", "upper");

		ArrayList<Row> toReturn = (ArrayList<Row>) bestTasas(ses, mapParameters);
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

	public static List<Row> seleccionarMejoresTasasPaisCompetidorRangoFechasRangoImporteNominal(String paisDestino, String divisa, String competidor, 
			String minYear, String minMonth, String minDay,
			String maxYear, String maxMonth, String maxDay,
			String minRangeImporteNominal, String maxRangeImporteNominal){
		final int importeDestinoId = importeDestinoIdDefault + 1;

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("pais_competidor_importe_nominal_query", paisDestino, divisa, minYear, minMonth, minDay);
		mapParameters.put("competidor", competidor);
		mapParameters.put("maxYear", maxYear);
		mapParameters.put("maxMonth", maxMonth);
		mapParameters.put("maxDay", maxDay);
		mapParameters.put("minRangeImporteNominal", minRangeImporteNominal);
		mapParameters.put("maxRangeImporteNominal", maxRangeImporteNominal);

		ArrayList<Row> toReturn = (ArrayList<Row>) bestTasas(ses, mapParameters);
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



	/* * * * * * * * * * * * * * * * * * * * * * * * * * * * *
	 *
	 * Queries for CIUDAD
	 * 
	 */

	public static List<Row> seleccionarMejoresTasasCiudadFechaConcreta(String paisDestino, String ciudad, String divisa, String year, String month, String day){

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("ciudad_query", paisDestino, divisa, year, month, day);			
		mapParameters.put("ciudad", ciudad);

		final int importeDestinoId = importeDestinoIdDefault;
		ArrayList<Row> toReturn = (ArrayList<Row>) bestTasas(ses, mapParameters);
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

	public static List<Row> seleccionarMejoresTasasCiudadFechaConcretaImporteNominalExacto(String paisDestino, String ciudad, String divisa, String year, 
			String month, String day, String importeNominal){
		final int importeDestinoId = importeDestinoIdDefault + 1;

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("ciudad_importe_nominal_query", paisDestino, divisa, year, month, day);	
		mapParameters.put("ciudad", ciudad);
		mapParameters.put("importeNominal", importeNominal);

		ArrayList<Row> toReturn = (ArrayList<Row>) bestTasas(ses, mapParameters);
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

	public static List<Row> seleccionarMejoresTasasCiudadFechaConcretaImporteNominalExactoNoExisteLower(String paisDestino, String ciudad, String divisa, String year, 
			String month, String day, String importeNominal){
		final int importeDestinoId = importeDestinoIdDefault + 1;

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("ciudad_importe_nominal_query", paisDestino, divisa, year, month, day);	
		mapParameters.put("ciudad", ciudad);
		mapParameters.put("importeNominal", importeNominal);
		mapParameters.put("searchFlag", "lower");

		ArrayList<Row> toReturn = (ArrayList<Row>) bestTasas(ses, mapParameters);
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

	public static List<Row> seleccionarMejoresTasasCiudadFechaConcretaImporteNominalExactoNoExisteUpper(String paisDestino, String ciudad, String divisa, String year, 
			String month, String day, String importeNominal){

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("ciudad_importe_nominal_query", paisDestino, divisa, year, month, day);	
		mapParameters.put("ciudad", ciudad);
		mapParameters.put("importeNominal", importeNominal);
		mapParameters.put("searchFlag", "upper");

		final int importeDestinoId = importeDestinoIdDefault + 1;
		ArrayList<Row> toReturn = (ArrayList<Row>) bestTasas(ses, mapParameters);
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

	public static List<Row> seleccionarMejoresTasasCiudadFechaConcretaRangoImporteNominal(String paisDestino, String ciudad, String divisa, String year, 
			String month, String day, String minRangeImporteNominal, String maxRangeImporteNominal){

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("ciudad_importe_nominal_query", paisDestino, divisa, year, month, day);	
		mapParameters.put("ciudad", ciudad);
		mapParameters.put("minRangeImporteNominal", minRangeImporteNominal);
		mapParameters.put("maxRangeImporteNominal", maxRangeImporteNominal);

		final int importeDestinoId = importeDestinoIdDefault + 1;
		ArrayList<Row> toReturn = (ArrayList<Row>) bestTasas(ses, mapParameters);
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

	public static List<Row> seleccionarMejoresTasasCiudadRangoFechas(String paisDestino, String ciudad, String divisa, 
			String minYear, String minMonth, String minDay,
			String maxYear, String maxMonth, String maxDay){

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("ciudad_importe_nominal_query", paisDestino, divisa, minYear, minMonth, minDay);	
		mapParameters.put("ciudad", ciudad);
		mapParameters.put("maxYear", maxYear);
		mapParameters.put("maxMonth", maxMonth);
		mapParameters.put("maxDay", maxDay);

		final int importeDestinoId = importeDestinoIdDefault;
		ArrayList<Row> toReturn = (ArrayList<Row>) bestTasas(ses, mapParameters);
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

	public static List<Row> seleccionarMejoresTasasCiudadRangoFechasImporteNominalExacto(String paisDestino, String ciudad, String divisa, 
			String minYear, String minMonth, String minDay,
			String maxYear, String maxMonth, String maxDay, 
			String importeNominal){

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("ciudad_importe_nominal_query", paisDestino, divisa, minYear, minMonth, minDay);
		mapParameters.put("ciudad", ciudad);
		mapParameters.put("divisa",  divisa);
		mapParameters.put("maxYear", maxYear);
		mapParameters.put("maxMonth", maxMonth);
		mapParameters.put("maxDay", maxDay);
		mapParameters.put("importeNominal", importeNominal);

		final int importeDestinoId = importeDestinoIdDefault + 1;
		ArrayList<Row> toReturn = (ArrayList<Row>) bestTasas(ses, mapParameters);
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

	public static List<Row> seleccionarMejoresTasasCiudadRangoFechasImporteNominalExactoNoExisteLower(String paisDestino, String ciudad, String divisa, 
			String minYear, String minMonth, String minDay,
			String maxYear, String maxMonth, String maxDay, 
			String importeNominal){

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("ciudad_importe_nominal_query", paisDestino, divisa, minYear, minMonth, minDay);
		mapParameters.put("ciudad", ciudad);
		mapParameters.put("maxYear", maxYear);
		mapParameters.put("maxMonth", maxMonth);
		mapParameters.put("maxDay", maxDay);
		mapParameters.put("importeNominal", importeNominal);
		mapParameters.put("searchFlag", "lower");

		final int importeDestinoId = importeDestinoIdDefault + 1;
		ArrayList<Row> toReturn = (ArrayList<Row>) bestTasas(ses, mapParameters);
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

	public static List<Row> seleccionarMejoresTasasCiudadRangoFechasImporteNominalExactoNoExisteUpper(String paisDestino, String ciudad, String divisa, 
			String minYear, String minMonth, String minDay,
			String maxYear, String maxMonth, String maxDay, 
			String importeNominal){

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("ciudad_importe_nominal_query", paisDestino, divisa, minYear, minMonth, minDay);
		mapParameters.put("ciudad", ciudad);
		mapParameters.put("maxYear", maxYear);
		mapParameters.put("maxMonth", maxMonth);
		mapParameters.put("maxDay", maxDay);
		mapParameters.put("importeNominal", importeNominal);
		mapParameters.put("searchFlag", "upper");

		final int importeDestinoId = importeDestinoIdDefault + 1;
		ArrayList<Row> toReturn = (ArrayList<Row>) bestTasas(ses, mapParameters);
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

	public static List<Row> seleccionarMejoresTasasCiudadRangoFechasRangoImporteNominal(String paisDestino, String ciudad, String divisa, 
			String minYear, String minMonth, String minDay,
			String maxYear, String maxMonth, String maxDay,
			String minRangeImporteNominal, String maxRangeImporteNominal){

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("ciudad_importe_nominal_query", paisDestino, divisa, minYear, minMonth, minDay);
		mapParameters.put("ciudad", ciudad);
		mapParameters.put("maxYear", maxYear);
		mapParameters.put("maxMonth", maxMonth);
		mapParameters.put("maxDay", maxDay);
		mapParameters.put("minRangeImporteNominal", minRangeImporteNominal);
		mapParameters.put("maxRangeImporteNominal", maxRangeImporteNominal);

		final int importeDestinoId = importeDestinoIdDefault + 1;
		ArrayList<Row> toReturn = (ArrayList<Row>) bestTasas(ses, mapParameters);
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

	public static List<Row> seleccionarMejoresTasasCiudadCompetidorFechaConcreta(String paisDestino, String ciudad, String divisa, String competidor, String year, String month, String day){

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("ciudad_competidor_query", paisDestino, divisa, year, month, day);
		mapParameters.put("competidor", competidor);
		mapParameters.put("ciudad", ciudad);

		final int importeDestinoId = importeDestinoIdDefault + 1;
		ArrayList<Row> toReturn = (ArrayList<Row>) bestTasas(ses, mapParameters);
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

	public static List<Row> seleccionarMejoresTasasCiudadCompetidorFechaConcretaImporteNominalExacto(String paisDestino, String ciudad, String divisa, String competidor, String year, 
			String month, String day, String importeNominal){

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("ciudad_competidor_importe_nominal_query", paisDestino, divisa, year, month, day);	
		mapParameters.put("competidor", competidor);
		mapParameters.put("ciudad", ciudad);
		mapParameters.put("importeNominal", importeNominal);

		final int importeDestinoId = importeDestinoIdDefault + 2;
		ArrayList<Row> toReturn = (ArrayList<Row>) bestTasas(ses, mapParameters);
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

	public static List<Row> seleccionarMejoresTasasCiudadCompetidorFechaConcretaImporteNominalExactoNoExisteLower(String paisDestino, String ciudad, String divisa, String competidor, String year, 
			String month, String day, String importeNominal){

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("ciudad_competidor_importe_nominal_query", paisDestino, divisa, year, month, day);	
		mapParameters.put("competidor", competidor);
		mapParameters.put("ciudad", ciudad);
		mapParameters.put("importeNominal", importeNominal);
		mapParameters.put("searchFlag", "lower");

		final int importeDestinoId = importeDestinoIdDefault + 2;
		ArrayList<Row> toReturn = (ArrayList<Row>) bestTasas(ses, mapParameters);
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

	public static List<Row> seleccionarMejoresTasasCiudadCompetidorFechaConcretaImporteNominalExactoNoExisteUpper(String paisDestino, String ciudad, String divisa, String competidor, String year, 
			String month, String day, String importeNominal){

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("ciudad_competidor_importe_nominal_query", paisDestino, divisa, year, month, day);	
		mapParameters.put("competidor", competidor);
		mapParameters.put("ciudad", ciudad);
		mapParameters.put("importeNominal", importeNominal);
		mapParameters.put("searchFlag", "upper");

		final int importeDestinoId = importeDestinoIdDefault + 2;
		ArrayList<Row> toReturn = (ArrayList<Row>) bestTasas(ses, mapParameters);
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

	public static List<Row> seleccionarMejoresTasasCiudadCompetidorFechaConcretaRangoImporteNominal(String paisDestino, String ciudad, String divisa, String competidor, String year, 
			String month, String day, String minRangeImporteNominal, String maxRangeImporteNominal){

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("ciudad_competidor_importe_nominal_query", paisDestino, divisa, year, month, day);	
		mapParameters.put("competidor", competidor);
		mapParameters.put("ciudad", ciudad);
		mapParameters.put("minRangeImporteNominal", minRangeImporteNominal);
		mapParameters.put("maxRangeImporteNominal", maxRangeImporteNominal);

		final int importeDestinoId = importeDestinoIdDefault + 2;
		ArrayList<Row> toReturn = (ArrayList<Row>) bestTasas(ses, mapParameters);
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

	public static List<Row> seleccionarMejoresTasasCiudadCompetidorRangoFechas(String paisDestino, String ciudad, String divisa, String competidor, 
			String minYear, String minMonth, String minDay,
			String maxYear, String maxMonth, String maxDay){

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("ciudad_competidor_importe_nominal_query", paisDestino, divisa, minYear, minMonth, minDay);	
		mapParameters.put("competidor", competidor);
		mapParameters.put("ciudad", ciudad);
		mapParameters.put("maxYear", maxYear);
		mapParameters.put("maxMonth", maxMonth);
		mapParameters.put("maxDay", maxDay);

		final int importeDestinoId = importeDestinoIdDefault + 1;
		ArrayList<Row> toReturn = (ArrayList<Row>) bestTasas(ses, mapParameters);
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

	public static List<Row> seleccionarMejoresTasasCiudadCompetidorRangoFechasImporteNominalExacto(String paisDestino, String ciudad, String divisa, String competidor, 
			String minYear, String minMonth, String minDay,
			String maxYear, String maxMonth, String maxDay, 
			String importeNominal){

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("ciudad_competidor_importe_nominal_query", paisDestino, divisa, minYear, minMonth, minDay);
		mapParameters.put("competidor", competidor);
		mapParameters.put("ciudad", ciudad);
		mapParameters.put("divisa",  divisa);
		mapParameters.put("maxYear", maxYear);
		mapParameters.put("maxMonth", maxMonth);
		mapParameters.put("maxDay", maxDay);
		mapParameters.put("importeNominal", importeNominal);

		final int importeDestinoId = importeDestinoIdDefault + 2;
		ArrayList<Row> toReturn = (ArrayList<Row>) bestTasas(ses, mapParameters);
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

	public static List<Row> seleccionarMejoresTasasCiudadCompetidorRangoFechasImporteNominalExactoNoExisteLower(String paisDestino, String ciudad, String divisa, String competidor, 
			String minYear, String minMonth, String minDay,
			String maxYear, String maxMonth, String maxDay, 
			String importeNominal){

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("ciudad_competidor_importe_nominal_query", paisDestino, divisa, minYear, minMonth, minDay);
		mapParameters.put("competidor", competidor);
		mapParameters.put("ciudad", ciudad);
		mapParameters.put("maxYear", maxYear);
		mapParameters.put("maxMonth", maxMonth);
		mapParameters.put("maxDay", maxDay);
		mapParameters.put("importeNominal", importeNominal);
		mapParameters.put("searchFlag", "lower");

		final int importeDestinoId = importeDestinoIdDefault + 2;
		ArrayList<Row> toReturn = (ArrayList<Row>) bestTasas(ses, mapParameters);
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

	public static List<Row> seleccionarMejoresTasasCiudadCompetidorRangoFechasImporteNominalExactoNoExisteUpper(String paisDestino, String ciudad, String divisa, String competidor, 
			String minYear, String minMonth, String minDay,
			String maxYear, String maxMonth, String maxDay, 
			String importeNominal){

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("ciudad_competidor_importe_nominal_query", paisDestino, divisa, minYear, minMonth, minDay);
		mapParameters.put("competidor", competidor);
		mapParameters.put("ciudad", ciudad);
		mapParameters.put("maxYear", maxYear);
		mapParameters.put("maxMonth", maxMonth);
		mapParameters.put("maxDay", maxDay);
		mapParameters.put("importeNominal", importeNominal);
		mapParameters.put("searchFlag", "upper");

		final int importeDestinoId = importeDestinoIdDefault + 2;
		ArrayList<Row> toReturn = (ArrayList<Row>) bestTasas(ses, mapParameters);
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

	public static List<Row> seleccionarMejoresTasasCiudadCompetidorRangoFechasRangoImporteNominal(String paisDestino, String ciudad, String divisa, String competidor, 
			String minYear, String minMonth, String minDay,
			String maxYear, String maxMonth, String maxDay,
			String minRangeImporteNominal, String maxRangeImporteNominal){

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("ciudad_competidor_importe_nominal_query", paisDestino, divisa, minYear, minMonth, minDay);
		mapParameters.put("competidor", competidor);
		mapParameters.put("ciudad", ciudad);
		mapParameters.put("maxYear", maxYear);
		mapParameters.put("maxMonth", maxMonth);
		mapParameters.put("maxDay", maxDay);
		mapParameters.put("minRangeImporteNominal", minRangeImporteNominal);
		mapParameters.put("maxRangeImporteNominal", maxRangeImporteNominal);

		final int importeDestinoId = importeDestinoIdDefault + 2;
		ArrayList<Row> toReturn = (ArrayList<Row>) bestTasas(ses, mapParameters);
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



	/* * * * * * * * * * * * * * * * * * * * * * * * * * * * *
	 *
	 * Queries for NUM_AGENTE 
	 * 
	 */

	public static List<Row> seleccionarMejoresTasasNumeroAgenteFechaConcreta(String paisDestino, String numAgente, String divisa, String year, String month, String day){

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("agente_query", paisDestino, divisa, year, month, day);		
		mapParameters.put("numAgente", numAgente);

		final int importeDestinoId = importeDestinoIdDefault;
		ArrayList<Row> toReturn = (ArrayList<Row>) bestTasas(ses, mapParameters);
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

	public static List<Row> seleccionarMejoresTasasNumeroAgenteFechaConcretaImporteNominalExacto(String paisDestino, String numAgente, String divisa, String year, 
			String month, String day, String importeNominal){

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("agente_importe_nominal_query", paisDestino, divisa, year, month, day);	
		mapParameters.put("numAgente", numAgente);	
		mapParameters.put("importeNominal", importeNominal);

		final int importeDestinoId = importeDestinoIdDefault + 1;
		ArrayList<Row> toReturn = (ArrayList<Row>) bestTasas(ses, mapParameters);
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

	public static List<Row> seleccionarMejoresTasasNumeroAgenteFechaConcretaImporteNominalExactoNoExisteLower(String paisDestino, String numAgente, String divisa, String year, 
			String month, String day, String importeNominal){

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("agente_importe_nominal_query", paisDestino, divisa, year, month, day);	
		mapParameters.put("numAgente", numAgente);
		mapParameters.put("importeNominal", importeNominal);
		mapParameters.put("searchFlag", "lower");

		final int importeDestinoId = importeDestinoIdDefault + 1;
		ArrayList<Row> toReturn = (ArrayList<Row>) bestTasas(ses, mapParameters);
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

	public static List<Row> seleccionarMejoresTasasNumeroAgenteFechaConcretaImporteNominalExactoNoExisteUpper(String paisDestino, String numAgente, String divisa, String year, 
			String month, String day, String importeNominal){

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("agente_importe_nominal_query", paisDestino, divisa, year, month, day);		
		mapParameters.put("numAgente", numAgente);
		mapParameters.put("importeNominal", importeNominal);
		mapParameters.put("searchFlag", "upper");

		final int importeDestinoId = importeDestinoIdDefault + 1;
		ArrayList<Row> toReturn = (ArrayList<Row>) bestTasas(ses, mapParameters);
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

	public static List<Row> seleccionarMejoresTasasNumeroAgenteFechaConcretaRangoImporteNominal(String paisDestino, String numAgente, String divisa, String year, 
			String month, String day, String minRangeImporteNominal, String maxRangeImporteNominal){

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("agente_importe_nominal_query", paisDestino, divisa, year, month, day);	
		mapParameters.put("numAgente", numAgente);
		mapParameters.put("minRangeImporteNominal", minRangeImporteNominal);
		mapParameters.put("maxRangeImporteNominal", maxRangeImporteNominal);

		final int importeDestinoId = importeDestinoIdDefault + 1;
		ArrayList<Row> toReturn = (ArrayList<Row>) bestTasas(ses, mapParameters);
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

	public static List<Row> seleccionarMejoresTasasNumeroAgenteRangoFechas(String paisDestino, String numAgente, String divisa, 
			String minYear, String minMonth, String minDay,
			String maxYear, String maxMonth, String maxDay){

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("agente_query", paisDestino, divisa, minYear, minMonth, minDay);
		mapParameters.put("numAgente", numAgente);
		mapParameters.put("maxYear", maxYear);
		mapParameters.put("maxMonth", maxMonth);
		mapParameters.put("maxDay", maxDay);
		
		final int importeDestinoId = importeDestinoIdDefault;
		ArrayList<Row> toReturn = (ArrayList<Row>) bestTasas(ses, mapParameters);
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

	public static List<Row> seleccionarMejoresTasasNumeroAgenteRangoFechasImporteNominalExacto(String paisDestino, String numAgente, String divisa, 
			String minYear, String minMonth, String minDay,
			String maxYear, String maxMonth, String maxDay, 
			String importeNominal){

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("agente_importe_nominal_query", paisDestino, divisa, minYear, minMonth, minDay);
		mapParameters.put("numAgente", numAgente);
		mapParameters.put("maxYear", maxYear);
		mapParameters.put("maxMonth", maxMonth);
		mapParameters.put("maxDay", maxDay);
		mapParameters.put("importeNominal", importeNominal);

		final int importeDestinoId = importeDestinoIdDefault + 1;
		ArrayList<Row> toReturn = (ArrayList<Row>) bestTasas(ses, mapParameters);
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

	public static List<Row> seleccionarMejoresTasasNumeroAgenteRangoFechasImporteNominalExactoNoExisteLower(String paisDestino, String numAgente, String divisa, 
			String minYear, String minMonth, String minDay,
			String maxYear, String maxMonth, String maxDay, 
			String importeNominal){

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("agente_importe_nominal_query", paisDestino, divisa, minYear, minMonth, minDay);	
		mapParameters.put("numAgente", numAgente);
		mapParameters.put("maxYear", maxYear);
		mapParameters.put("maxMonth", maxMonth);
		mapParameters.put("maxDay", maxDay);
		mapParameters.put("importeNominal", importeNominal);
		mapParameters.put("searchFlag", "lower");

		final int importeDestinoId = importeDestinoIdDefault + 1;
		ArrayList<Row> toReturn = (ArrayList<Row>) bestTasas(ses, mapParameters);
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

	public static List<Row> seleccionarMejoresTasasNumeroAgenteRangoFechasImporteNominalExactoNoExisteUpper(String paisDestino, String numAgente, String divisa, 
			String minYear, String minMonth, String minDay,
			String maxYear, String maxMonth, String maxDay, 
			String importeNominal){

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("agente_importe_nominal_query", paisDestino, divisa, minYear, minMonth, minDay);
		mapParameters.put("numAgente", numAgente);
		mapParameters.put("maxYear", maxYear);
		mapParameters.put("maxMonth", maxMonth);
		mapParameters.put("maxDay", maxDay);
		mapParameters.put("importeNominal", importeNominal);
		mapParameters.put("searchFlag", "upper");

		final int importeDestinoId = importeDestinoIdDefault + 1;
		ArrayList<Row> toReturn = (ArrayList<Row>) bestTasas(ses, mapParameters);
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

	public static List<Row> seleccionarMejoresTasasNumeroAgenteRangoFechasRangoImporteNominal(String paisDestino, String numAgente, String divisa, 
			String minYear, String minMonth, String minDay,
			String maxYear, String maxMonth, String maxDay,
			String minRangeImporteNominal, String maxRangeImporteNominal){

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("agente_importe_nominal_query", paisDestino, divisa, minYear, minMonth, minDay);
		mapParameters.put("numAgente", numAgente);
		mapParameters.put("maxYear", maxYear);
		mapParameters.put("maxMonth", maxMonth);
		mapParameters.put("maxDay", maxDay);
		mapParameters.put("minRangeImporteNominal", minRangeImporteNominal);
		mapParameters.put("maxRangeImporteNominal", maxRangeImporteNominal);

		final int importeDestinoId = importeDestinoIdDefault + 1;
		ArrayList<Row> toReturn = (ArrayList<Row>) bestTasas(ses, mapParameters);
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

	public static List<Row> seleccionarMejoresTasasNumeroAgenteCompetidorFechaConcreta(String paisDestino, String numAgente, String divisa, String competidor, String year, String month, String day){

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("agente_competidor__query", paisDestino, divisa, year, month, day);	
		mapParameters.put("competidor",  competidor);
		mapParameters.put("numAgente", numAgente);

		final int importeDestinoId = importeDestinoIdDefault + 1;
		ArrayList<Row> toReturn = (ArrayList<Row>) bestTasas(ses, mapParameters);
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

	public static List<Row> seleccionarMejoresTasasNumeroAgenteCompetidorFechaConcretaImporteNominalExacto(String paisDestino, String numAgente, String divisa, String competidor, String year, 
			String month, String day, String importeNominal){

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("agente_competidor_importe_nominal_query", paisDestino, divisa, year, month, day);	
		mapParameters.put("competidor",  competidor);
		mapParameters.put("numAgente", numAgente);	
		mapParameters.put("importeNominal", importeNominal);

		final int importeDestinoId = importeDestinoIdDefault + 2;
		ArrayList<Row> toReturn = (ArrayList<Row>) bestTasas(ses, mapParameters);
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

	public static List<Row> seleccionarMejoresTasasNumeroAgenteCompetidorFechaConcretaImporteNominalExactoNoExisteLower(String paisDestino, String numAgente, String divisa, String competidor, String year, 
			String month, String day, String importeNominal){

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("agente_competidor_importe_nominal_query", paisDestino, divisa, year, month, day);	
		mapParameters.put("competidor",  competidor);
		mapParameters.put("numAgente", numAgente);
		mapParameters.put("importeNominal", importeNominal);
		mapParameters.put("searchFlag", "lower");

		final int importeDestinoId = importeDestinoIdDefault + 2;
		ArrayList<Row> toReturn = (ArrayList<Row>) bestTasas(ses, mapParameters);
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

	public static List<Row> seleccionarMejoresTasasNumeroAgenteCompetidorFechaConcretaImporteNominalExactoNoExisteUpper(String paisDestino, String numAgente, String divisa, String competidor, String year, 
			String month, String day, String importeNominal){

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("agente_competidor_importe_nominal_query", paisDestino, divisa, year, month, day);	
		mapParameters.put("competidor",  competidor);
		mapParameters.put("numAgente", numAgente);
		mapParameters.put("importeNominal", importeNominal);
		mapParameters.put("searchFlag", "upper");

		final int importeDestinoId = importeDestinoIdDefault + 2;
		ArrayList<Row> toReturn = (ArrayList<Row>) bestTasas(ses, mapParameters);
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

	public static List<Row> seleccionarMejoresTasasNumeroAgenteCompetidorFechaConcretaRangoImporteNominal(String paisDestino, String numAgente, String divisa, String competidor, String year, 
			String month, String day, String minRangeImporteNominal, String maxRangeImporteNominal){

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("agente_competidor_importe_nominal_query", paisDestino, divisa, year, month, day);	
		mapParameters.put("competidor",  competidor);
		mapParameters.put("numAgente", numAgente);
		mapParameters.put("minRangeImporteNominal", minRangeImporteNominal);
		mapParameters.put("maxRangeImporteNominal", maxRangeImporteNominal);

		final int importeDestinoId = importeDestinoIdDefault + 2;
		ArrayList<Row> toReturn = (ArrayList<Row>) bestTasas(ses, mapParameters);
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

	public static List<Row> seleccionarMejoresTasasNumeroAgenteCompetidorRangoFechas(String paisDestino, String numAgente, String divisa, String competidor, 
			String minYear, String minMonth, String minDay,
			String maxYear, String maxMonth, String maxDay){

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("agente_competidor_importe_nominal_query", paisDestino, divisa, minYear, minMonth, minDay);
		mapParameters.put("competidor",  competidor);
		mapParameters.put("numAgente", numAgente);
		mapParameters.put("maxYear", maxYear);
		mapParameters.put("maxMonth", maxMonth);
		mapParameters.put("maxDay", maxDay);

		final int importeDestinoId = importeDestinoIdDefault + 1;
		ArrayList<Row> toReturn = (ArrayList<Row>) bestTasas(ses, mapParameters);
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

	public static List<Row> seleccionarMejoresTasasNumeroAgenteCompetidorRangoFechasImporteNominalExacto(String paisDestino, String numAgente, String divisa, String competidor, 
			String minYear, String minMonth, String minDay,
			String maxYear, String maxMonth, String maxDay, 
			String importeNominal){

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("agente_competidor_importe_nominal_query", paisDestino, divisa, minYear, minMonth, minDay);
		mapParameters.put("competidor",  competidor);
		mapParameters.put("numAgente", numAgente);
		mapParameters.put("maxYear", maxYear);
		mapParameters.put("maxMonth", maxMonth);
		mapParameters.put("maxDay", maxDay);
		mapParameters.put("importeNominal", importeNominal);

		final int importeDestinoId = importeDestinoIdDefault + 2;
		ArrayList<Row> toReturn = (ArrayList<Row>) bestTasas(ses, mapParameters);
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

	public static List<Row> seleccionarMejoresTasasNumeroAgenteCompetidorRangoFechasImporteNominalExactoNoExisteLower(String paisDestino, String numAgente, String divisa, String competidor, 
			String minYear, String minMonth, String minDay,
			String maxYear, String maxMonth, String maxDay, 
			String importeNominal){

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("agente_competidor_importe_nominal_query", paisDestino, divisa, minYear, minMonth, minDay);
		mapParameters.put("competidor",  competidor);
		mapParameters.put("numAgente", numAgente);
		mapParameters.put("maxYear", maxYear);
		mapParameters.put("maxMonth", maxMonth);
		mapParameters.put("maxDay", maxDay);
		mapParameters.put("importeNominal", importeNominal);
		mapParameters.put("searchFlag", "lower");

		final int importeDestinoId = importeDestinoIdDefault + 2;
		ArrayList<Row> toReturn = (ArrayList<Row>) bestTasas(ses, mapParameters);
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

	public static List<Row> seleccionarMejoresTasasNumeroAgenteCompetidorRangoFechasImporteNominalExactoNoExisteUpper(String paisDestino, String numAgente, String divisa, String competidor, 
			String minYear, String minMonth, String minDay,
			String maxYear, String maxMonth, String maxDay, 
			String importeNominal){

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("agente_competidor_importe_nominal_query", paisDestino, divisa, minYear, minMonth, minDay);
		mapParameters.put("competidor",  competidor);
		mapParameters.put("numAgente", numAgente);
		mapParameters.put("maxYear", maxYear);
		mapParameters.put("maxMonth", maxMonth);
		mapParameters.put("maxDay", maxDay);
		mapParameters.put("importeNominal", importeNominal);
		mapParameters.put("searchFlag", "upper");

		final int importeDestinoId = importeDestinoIdDefault + 2;
		ArrayList<Row> toReturn = (ArrayList<Row>) bestTasas(ses, mapParameters);
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

	public static List<Row> seleccionarMejoresTasasNumeroAgenteCompetidorRangoFechasRangoImporteNominal(String paisDestino, String numAgente, String divisa, String competidor, 
			String minYear, String minMonth, String minDay,
			String maxYear, String maxMonth, String maxDay,
			String minRangeImporteNominal, String maxRangeImporteNominal){

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("agente_competidor_importe_nominal_query", paisDestino, divisa, minYear, minMonth, minDay);
		mapParameters.put("competidor",  competidor);
		mapParameters.put("numAgente", numAgente);
		mapParameters.put("maxYear", maxYear);
		mapParameters.put("maxMonth", maxMonth);
		mapParameters.put("maxDay", maxDay);
		mapParameters.put("minRangeImporteNominal", minRangeImporteNominal);
		mapParameters.put("maxRangeImporteNominal", maxRangeImporteNominal);

		final int importeDestinoId = importeDestinoIdDefault + 2;
		ArrayList<Row> toReturn = (ArrayList<Row>) bestTasas(ses, mapParameters);
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



	/* * * * * * * * * * * * * * * * * * * * * * * * * * * * *
	 *
	 * Queries for GEOLOCALIZACION
	 * 
	 */


	public static List<Row> seleccionarGeolocalizacionDistancia(String paisDestino, String lat, String lon, String distancia, String divisa){

		Session ses = connect();
		GeoLocation location = GeoLocation.fromDegrees(Double.parseDouble(lat), Double.parseDouble(lon));
		GeoLocation[] boundingCoordinates =
				location.boundingCoordinates(Double.parseDouble(distancia));

		Double minLat = boundingCoordinates[0].getLatitudeInDegrees();
		Double minLon = boundingCoordinates[0].getLongitudeInDegrees();
		Double maxLat = boundingCoordinates[1].getLatitudeInDegrees();
		Double maxLon = boundingCoordinates[1].getLongitudeInDegrees();

		String maxGeohash = GeoHash.geoHashStringWithCharacterPrecision(maxLat, maxLon, 12);
		String minGeoHash = GeoHash.geoHashStringWithCharacterPrecision(minLat, minLon, 12);

		HashMap<String, String> mapParameters = defineMapParameters("geohash_scheme", paisDestino, divisa, "", "", "");		
		mapParameters.put("maxGeohash",  maxGeohash);
		mapParameters.put("minGeohash", minGeoHash);

		return bestTasaGivenDate(ses, mapParameters);
	}

	public static List<Row> seleccionarMejoresTasasGeolocalizacionFechaConcreta(String paisDestino, String lat, String lon, String distancia, 
			String divisa, String year, String month, String day){

		Session ses = connect();
		List<Row> geolocalizaciones = seleccionarGeolocalizacionDistancia(paisDestino, lat, lon, distancia, divisa);
		HashMap<String, String> mapParameters = defineMapParameters("geohash_query", paisDestino, divisa, year, month, day);

		ArrayList<Row> toReturn = new ArrayList<Row>();
		for(int i=0; i<geolocalizaciones.size(); i++){
			String geohash = geolocalizaciones.get(i).getString(0);

			mapParameters.put("geohash", geohash);
			toReturn.addAll(bestTasas(ses, mapParameters));
		}

		final int importeDestinoId = importeDestinoIdDefault;
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

	public static List<Row> seleccionarMejoresTasasGeolocalizacionFechaConcretaImporteNominalExacto(String paisDestino, String lat, String lon, String distancia, 
			String divisa, String year, 
			String month, String day, String importeNominal){

		Session ses = connect();
		List<Row> geolocalizaciones = seleccionarGeolocalizacionDistancia(paisDestino, lat, lon, distancia, divisa);
		HashMap<String, String> mapParameters = defineMapParameters("geohash_importe_nominal_query", paisDestino, divisa, year, month, day);	
		mapParameters.put("importeNominal", importeNominal);

		List<Row> toReturn = new ArrayList<Row>();
		for(int i=0; i<geolocalizaciones.size(); i++){
			String geohash = geolocalizaciones.get(i).getString(0);

			mapParameters.put("geohash", geohash);
			toReturn.addAll(bestTasas(ses, mapParameters));
		}

		final int importeDestinoId = importeDestinoIdDefault + 1;
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

	public static List<Row> seleccionarMejoresTasasGeolocalizacionFechaConcretaImporteNominalExactoNoExisteLower(String paisDestino, String lat, String lon, String distancia, 
			String divisa, String year, 
			String month, String day, String importeNominal){

		Session ses = connect();
		List<Row> geolocalizaciones = seleccionarGeolocalizacionDistancia(paisDestino, lat, lon, distancia, divisa);
		HashMap<String, String> mapParameters = defineMapParameters("geohash_importe_nominal_query", paisDestino, divisa, year, month, day);	
		mapParameters.put("importeNominal", importeNominal);

		List<Row> toReturn = new ArrayList<Row>();
		for(int i=0; i<geolocalizaciones.size(); i++){
			String geohash = geolocalizaciones.get(i).getString(0);

			mapParameters.put("searchFlag", "lower");
			mapParameters.put("geohash", geohash);
			toReturn.addAll(bestTasas(ses, mapParameters));
		}

		final int importeDestinoId = importeDestinoIdDefault + 1;
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

	public static List<Row> seleccionarMejoresTasasGeolocalizacionFechaConcretaImporteNominalExactoNoExisteUpper(String paisDestino, String lat, String lon, String distancia, 
			String divisa, String year, 
			String month, String day, String importeNominal){

		Session ses = connect();
		List<Row> geolocalizaciones = seleccionarGeolocalizacionDistancia(paisDestino, lat, lon, distancia, divisa);
		HashMap<String, String> mapParameters = defineMapParameters("geohash_importe_nominal_query", paisDestino, divisa, year, month, day);	
		mapParameters.put("importeNominal", importeNominal);

		List<Row> toReturn = new ArrayList<Row>();
		for(int i=0; i<geolocalizaciones.size(); i++){
			String geohash = geolocalizaciones.get(i).getString(0);

			mapParameters.put("searchFlag", "upper");
			mapParameters.put("geohash", geohash);
			toReturn.addAll(bestTasas(ses, mapParameters));
		}

		final int importeDestinoId = importeDestinoIdDefault + 1;
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

	public static List<Row> seleccionarMejoresTasasGeolocalizacionFechaConcretaRangoImporteNominal(String paisDestino, String lat, String lon, String distancia, 
			String divisa, String year, 
			String month, String day, String minRangeImporteNominal, String maxRangeImporteNominal){

		Session ses = connect();
		List<Row> geolocalizaciones = seleccionarGeolocalizacionDistancia(paisDestino, lat, lon, distancia, divisa);
		HashMap<String, String> mapParameters = defineMapParameters("geohash_importe_nominal_query", paisDestino, divisa, year, month, day);	
		mapParameters.put("minRangeImporteNominal", minRangeImporteNominal);
		mapParameters.put("maxRangeImporteNominal", maxRangeImporteNominal);

		List<Row> toReturn = new ArrayList<Row>();
		for(int i=0; i<geolocalizaciones.size(); i++){
			String geohash = geolocalizaciones.get(i).getString(0);
			mapParameters.put("geohash", geohash);
			toReturn.addAll(bestTasas(ses, mapParameters));
		}

		final int importeDestinoId = importeDestinoIdDefault + 1;
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

	public static List<Row> seleccionarMejoresTasasGeolocalizacionRangoFechas(String paisDestino, String lat, String lon, String distancia, 
			String divisa, 
			String minYear, String minMonth, String minDay,
			String maxYear, String maxMonth, String maxDay){

		Session ses = connect();
		List<Row> geolocalizaciones = seleccionarGeolocalizacionDistancia(paisDestino, lat, lon, distancia, divisa);
		HashMap<String, String> mapParameters = defineMapParameters("geohash_importe_nominal_query", paisDestino, divisa, minYear, minMonth, minDay);
		mapParameters.put("maxYear", maxYear);
		mapParameters.put("maxMonth", maxMonth);
		mapParameters.put("maxDay", maxDay);

		List<Row> toReturn = new ArrayList<Row>();
		for(int i=0; i<geolocalizaciones.size(); i++){
			String geohash = geolocalizaciones.get(i).getString(0);
			mapParameters.put("geohash", geohash);
			mapParameters.put("year", minYear);
			mapParameters.put("month", minMonth);
			mapParameters.put("day", minDay);
			toReturn.addAll(bestTasas(ses, mapParameters));
		}

		final int importeDestinoId = importeDestinoIdDefault;
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



	public static List<Row> seleccionarMejoresTasasGeolocalizacionRangoFechasImporteNominalExacto(String paisDestino, String lat, String lon, String distancia,
			String divisa, 
			String minYear, String minMonth, String minDay,
			String maxYear, String maxMonth, String maxDay, 
			String importeNominal){

		Session ses = connect();
		List<Row> geolocalizaciones = seleccionarGeolocalizacionDistancia(paisDestino, lat, lon, distancia, divisa);
		HashMap<String, String> mapParameters = defineMapParameters("geohash_importe_nominal_query", paisDestino, divisa, minYear, minMonth, minDay);
		mapParameters.put("maxYear", maxYear);
		mapParameters.put("maxMonth", maxMonth);
		mapParameters.put("maxDay", maxDay);
		mapParameters.put("importeNominal", importeNominal);

		List<Row> toReturn = new ArrayList<Row>();
		for(int i=0; i<geolocalizaciones.size(); i++){
			String geohash = geolocalizaciones.get(i).getString(0);
			mapParameters.put("geohash", geohash);
			mapParameters.put("year", minYear);
			mapParameters.put("month", minMonth);
			mapParameters.put("day", minDay);
			toReturn.addAll(bestTasas(ses, mapParameters));
		}

		final int importeDestinoId = importeDestinoIdDefault + 1;
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

	public static List<Row> seleccionarMejoresTasasGeolocalizacionRangoFechasImporteNominalExactoNoExisteLower(String paisDestino,  String lat, String lon, String distancia,
			String divisa, 
			String minYear, String minMonth, String minDay,
			String maxYear, String maxMonth, String maxDay, 
			String importeNominal){

		Session ses = connect();
		List<Row> geolocalizaciones = seleccionarGeolocalizacionDistancia(paisDestino, lat, lon, distancia, divisa);
		HashMap<String, String> mapParameters = defineMapParameters("geohash_importe_nominal_query", paisDestino, divisa, minYear, minMonth, minDay);
		mapParameters.put("maxYear", maxYear);
		mapParameters.put("maxMonth", maxMonth);
		mapParameters.put("maxDay", maxDay);
		mapParameters.put("importeNominal", importeNominal);

		List<Row> toReturn = new ArrayList<Row>();
		for(int i=0; i<geolocalizaciones.size(); i++){
			String geohash = geolocalizaciones.get(i).getString(0);
			mapParameters.put("geohash", geohash);
			mapParameters.put("year", minYear);
			mapParameters.put("month", minMonth);
			mapParameters.put("day", minDay);
			mapParameters.put("searchFlag", "lower");
			toReturn.addAll(bestTasas(ses, mapParameters));
		}

		final int importeDestinoId = importeDestinoIdDefault + 1;
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

	public static List<Row> seleccionarMejoresTasasGeolocalizacionRangoFechasImporteNominalExactoNoExisteUpper(String paisDestino,  String lat, String lon, String distancia,
			String divisa, 
			String minYear, String minMonth, String minDay,
			String maxYear, String maxMonth, String maxDay, 
			String importeNominal){

		Session ses = connect();
		List<Row> geolocalizaciones = seleccionarGeolocalizacionDistancia(paisDestino, lat, lon, distancia, divisa);
		HashMap<String, String> mapParameters = defineMapParameters("geohash_importe_nominal_query", paisDestino, divisa, minYear, minMonth, minDay);
		mapParameters.put("maxYear", maxYear);
		mapParameters.put("maxMonth", maxMonth);
		mapParameters.put("maxDay", maxDay);
		mapParameters.put("importeNominal", importeNominal);

		List<Row> toReturn = new ArrayList<Row>();
		for(int i=0; i<geolocalizaciones.size(); i++){
			String geohash = geolocalizaciones.get(i).getString(0);
			mapParameters.put("geohash", geohash);
			mapParameters.put("year", minYear);
			mapParameters.put("month", minMonth);
			mapParameters.put("day", minDay);
			mapParameters.put("searchFlag", "upper");
			toReturn.addAll(bestTasas(ses, mapParameters));
		}

		final int importeDestinoId = importeDestinoIdDefault + 1;
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

	public static List<Row> seleccionarMejoresTasasGeolocalizacionRangoFechasRangoImporteNominal(String paisDestino, String lat, String lon, String distancia,
			String divisa, 
			String minYear, String minMonth, String minDay,
			String maxYear, String maxMonth, String maxDay,
			String minRangeImporteNominal, String maxRangeImporteNominal){

		Session ses = connect();
		List<Row> geolocalizaciones = seleccionarGeolocalizacionDistancia(paisDestino, lat, lon, distancia, divisa);
		HashMap<String, String> mapParameters = defineMapParameters("geohash_importe_nominal_query", paisDestino, divisa, minYear, minMonth, minDay);
		mapParameters.put("maxYear", maxYear);
		mapParameters.put("maxMonth", maxMonth);
		mapParameters.put("maxDay", maxDay);
		mapParameters.put("minRangeImporteNominal", minRangeImporteNominal);
		mapParameters.put("maxRangeImporteNominal", maxRangeImporteNominal);

		List<Row> toReturn = new ArrayList<Row>();
		for(int i=0; i<geolocalizaciones.size(); i++){
			String geohash = geolocalizaciones.get(i).getString(0);
			mapParameters.put("geohash", geohash);
			mapParameters.put("year", minYear);
			mapParameters.put("month", minMonth);
			mapParameters.put("day", minDay);
			toReturn.addAll(bestTasas(ses, mapParameters));
		}

		final int importeDestinoId = importeDestinoIdDefault + 1;
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

	public static List<Row> seleccionarGeolocalizacionCompetidorDistancia(String paisDestino, String lat, String lon, String distancia, String divisa, String competidor){

		Session ses = connect();
		GeoLocation location = GeoLocation.fromDegrees(Double.parseDouble(lat), Double.parseDouble(lon));
		GeoLocation[] boundingCoordinates =
				location.boundingCoordinates(Double.parseDouble(distancia));

		Double minLat = boundingCoordinates[0].getLatitudeInDegrees();
		Double minLon = boundingCoordinates[0].getLongitudeInDegrees();
		Double maxLat = boundingCoordinates[1].getLatitudeInDegrees();
		Double maxLon = boundingCoordinates[1].getLongitudeInDegrees();

		String maxGeohash = GeoHash.geoHashStringWithCharacterPrecision(maxLat, maxLon, 12);
		String minGeoHash = GeoHash.geoHashStringWithCharacterPrecision(minLat, minLon, 12);

		HashMap<String, String> mapParameters = defineMapParameters("geohash_competidor_scheme", paisDestino, divisa, "", "", "");	
		mapParameters.put("competidor", competidor);
		mapParameters.put("maxGeohash",  maxGeohash);
		mapParameters.put("minGeohash", minGeoHash);

		return bestTasaGivenDate(ses, mapParameters);
	}

	public static List<Row> seleccionarMejoresTasasGeolocalizacionCompetidorFechaConcreta(String paisDestino, String lat, String lon, String distancia, 
			String divisa, String competidor, String year, String month, String day){

		Session ses = connect();
		List<Row> geolocalizaciones = seleccionarGeolocalizacionCompetidorDistancia(paisDestino, lat, lon, distancia, divisa, competidor);
		HashMap<String, String> mapParameters = defineMapParameters("geohash_competidor_query", paisDestino, divisa, year, month, day);
		mapParameters.put("competidor", competidor);

		List<Row> toReturn = new ArrayList<Row>();
		for(int i=0; i<geolocalizaciones.size(); i++){
			String geohash = geolocalizaciones.get(i).getString(0);

			mapParameters.put("geohash", geohash);
			toReturn.addAll(bestTasas(ses, mapParameters));
		}

		final int importeDestinoId = importeDestinoIdDefault + 1;
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

	public static List<Row> seleccionarMejoresTasasGeolocalizacionCompetidorFechaConcretaImporteNominalExacto(String paisDestino, String lat, String lon, String distancia, 
			String divisa, String competidor, String year, 
			String month, String day, String importeNominal){

		Session ses = connect();
		List<Row> geolocalizaciones = seleccionarGeolocalizacionCompetidorDistancia(paisDestino, lat, lon, distancia, divisa, competidor);
		HashMap<String, String> mapParameters = defineMapParameters("geohash_competidor_importe_nominal_query", paisDestino, divisa, year, month, day);	
		mapParameters.put("competidor", competidor);
		mapParameters.put("importeNominal", importeNominal);

		List<Row> toReturn = new ArrayList<Row>();
		for(int i=0; i<geolocalizaciones.size(); i++){
			String geohash = geolocalizaciones.get(i).getString(0);

			mapParameters.put("geohash", geohash);
			toReturn.addAll(bestTasas(ses, mapParameters));
		}

		final int importeDestinoId = importeDestinoIdDefault + 2;
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

	public static List<Row> seleccionarMejoresTasasGeolocalizacionCompetidorFechaConcretaImporteNominalExactoNoExisteLower(String paisDestino, String lat, String lon, String distancia, 
			String divisa, String competidor, String year, 
			String month, String day, String importeNominal){

		Session ses = connect();
		List<Row> geolocalizaciones = seleccionarGeolocalizacionCompetidorDistancia(paisDestino, lat, lon, distancia, divisa, competidor);
		HashMap<String, String> mapParameters = defineMapParameters("geohash_competidor_importe_nominal_query", paisDestino, divisa, year, month, day);
		mapParameters.put("competidor", competidor);
		mapParameters.put("importeNominal", importeNominal);

		List<Row> toReturn = new ArrayList<Row>();
		for(int i=0; i<geolocalizaciones.size(); i++){
			String geohash = geolocalizaciones.get(i).getString(0);

			mapParameters.put("searchFlag", "lower");
			mapParameters.put("geohash", geohash);
			toReturn.addAll(bestTasas(ses, mapParameters));
		}
		final int importeDestinoId = importeDestinoIdDefault + 2;
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

	public static List<Row> seleccionarMejoresTasasGeolocalizacionCompetidorFechaConcretaImporteNominalExactoNoExisteUpper(String paisDestino, String lat, String lon, String distancia, 
			String divisa, String competidor, String year, 
			String month, String day, String importeNominal){

		Session ses = connect();
		List<Row> geolocalizaciones = seleccionarGeolocalizacionCompetidorDistancia(paisDestino, lat, lon, distancia, divisa, competidor);
		HashMap<String, String> mapParameters = defineMapParameters("geohash_competidor_importe_nominal_query", paisDestino, divisa, year, month, day);	
		mapParameters.put("competidor", competidor);
		mapParameters.put("importeNominal", importeNominal);

		List<Row> toReturn = new ArrayList<Row>();
		for(int i=0; i<geolocalizaciones.size(); i++){
			String geohash = geolocalizaciones.get(i).getString(0);

			mapParameters.put("searchFlag", "upper");
			mapParameters.put("geohash", geohash);
			toReturn.addAll(bestTasas(ses, mapParameters));
		}
		final int importeDestinoId = importeDestinoIdDefault + 2;
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

	public static List<Row> seleccionarMejoresTasasGeolocalizacionCompetidorFechaConcretaRangoImporteNominal(String paisDestino, String lat, String lon, String distancia, 
			String divisa, String competidor, String year, 
			String month, String day, String minRangeImporteNominal, String maxRangeImporteNominal){

		Session ses = connect();
		List<Row> geolocalizaciones = seleccionarGeolocalizacionCompetidorDistancia(paisDestino, lat, lon, distancia, divisa, competidor);
		HashMap<String, String> mapParameters = defineMapParameters("geohash_competidor_importe_nominal_query", paisDestino, divisa, year, month, day);	
		mapParameters.put("competidor", competidor);
		mapParameters.put("minRangeImporteNominal", minRangeImporteNominal);
		mapParameters.put("maxRangeImporteNominal", maxRangeImporteNominal);

		List<Row> toReturn = new ArrayList<Row>();
		for(int i=0; i<geolocalizaciones.size(); i++){
			String geohash = geolocalizaciones.get(i).getString(0);
			mapParameters.put("geohash", geohash);
			toReturn.addAll(bestTasas(ses, mapParameters));
		}
		final int importeDestinoId = importeDestinoIdDefault + 2;
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

	public static List<Row> seleccionarMejoresTasasGeolocalizacionCompetidorRangoFechas(String paisDestino, String lat, String lon, String distancia, 
			String divisa, String competidor, 
			String minYear, String minMonth, String minDay,
			String maxYear, String maxMonth, String maxDay){

		Session ses = connect();
		List<Row> geolocalizaciones = seleccionarGeolocalizacionCompetidorDistancia(paisDestino, lat, lon, distancia, divisa, competidor);
		HashMap<String, String> mapParameters = defineMapParameters("geohash_competidor_importe_nominal_query", paisDestino, divisa, minYear, minMonth, minDay);
		mapParameters.put("competidor", competidor);
		mapParameters.put("maxYear", maxYear);
		mapParameters.put("maxMonth", maxMonth);
		mapParameters.put("maxDay", maxDay);

		List<Row> toReturn = new ArrayList<Row>();
		for(int i=0; i<geolocalizaciones.size(); i++){
			String geohash = geolocalizaciones.get(i).getString(0);
			mapParameters.put("geohash", geohash);
			mapParameters.put("year", minYear);
			mapParameters.put("month", minMonth);
			mapParameters.put("day", minDay);
			toReturn.addAll(bestTasas(ses, mapParameters));
		}
		final int importeDestinoId = importeDestinoIdDefault + 1;
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

	public static List<Row> seleccionarMejoresTasasNumeroAgenteCompetidorRangoFechasImporteNominalExacto(String paisDestino, String lat, String lon, String distancia,
			String divisa, String competidor, 
			String minYear, String minMonth, String minDay,
			String maxYear, String maxMonth, String maxDay, 
			String importeNominal){

		Session ses = connect();
		List<Row> geolocalizaciones = seleccionarGeolocalizacionCompetidorDistancia(paisDestino, lat, lon, distancia, divisa, competidor);
		HashMap<String, String> mapParameters = defineMapParameters("geohash_competidor_importe_nominal_query", paisDestino, divisa, minYear, minMonth, minDay);
		mapParameters.put("competidor", competidor);
		mapParameters.put("maxYear", maxYear);
		mapParameters.put("maxMonth", maxMonth);
		mapParameters.put("maxDay", maxDay);
		mapParameters.put("importeNominal", importeNominal);

		List<Row> toReturn = new ArrayList<Row>();
		for(int i=0; i<geolocalizaciones.size(); i++){
			String geohash = geolocalizaciones.get(i).getString(0);
			mapParameters.put("geohash", geohash);
			mapParameters.put("year", minYear);
			mapParameters.put("month", minMonth);
			mapParameters.put("day", minDay);
			toReturn.addAll(bestTasas(ses, mapParameters));
		}
		final int importeDestinoId = importeDestinoIdDefault + 2;
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

	public static List<Row> seleccionarMejoresTasasGeolocalizacionCompetidorRangoFechasImporteNominalExactoNoExisteLower(String paisDestino,  String lat, String lon, String distancia,
			String divisa, String competidor, 
			String minYear, String minMonth, String minDay,
			String maxYear, String maxMonth, String maxDay, 
			String importeNominal){

		Session ses = connect();
		List<Row> geolocalizaciones = seleccionarGeolocalizacionCompetidorDistancia(paisDestino, lat, lon, distancia, divisa, competidor);
		HashMap<String, String> mapParameters = defineMapParameters("geohash_competidor_importe_nominal_query", paisDestino, divisa, minYear, minMonth, minDay);
		mapParameters.put("competidor", competidor);
		mapParameters.put("maxYear", maxYear);
		mapParameters.put("maxMonth", maxMonth);
		mapParameters.put("maxDay", maxDay);
		mapParameters.put("importeNominal", importeNominal);

		List<Row> toReturn = new ArrayList<Row>();
		for(int i=0; i<geolocalizaciones.size(); i++){
			String geohash = geolocalizaciones.get(i).getString(0);
			mapParameters.put("geohash", geohash);
			mapParameters.put("year", minYear);
			mapParameters.put("month", minMonth);
			mapParameters.put("day", minDay);
			mapParameters.put("searchFlag", "lower");
			toReturn.addAll(bestTasas(ses, mapParameters));
		}
		final int importeDestinoId = importeDestinoIdDefault + 2;
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

	public static List<Row> seleccionarMejoresTasasGeolocalizacionCompetidorRangoFechasImporteNominalExactoNoExisteUpper(String paisDestino,  String lat, String lon, String distancia,
			String divisa, String competidor, 
			String minYear, String minMonth, String minDay,
			String maxYear, String maxMonth, String maxDay, 
			String importeNominal){

		Session ses = connect();
		List<Row> geolocalizaciones = seleccionarGeolocalizacionCompetidorDistancia(paisDestino, lat, lon, distancia, divisa, competidor);
		HashMap<String, String> mapParameters = defineMapParameters("geohash_competidor_importe_nominal_query", paisDestino, divisa, minYear, minMonth, minDay);
		mapParameters.put("competidor", competidor);
		mapParameters.put("maxYear", maxYear);
		mapParameters.put("maxMonth", maxMonth);
		mapParameters.put("maxDay", maxDay);
		mapParameters.put("importeNominal", importeNominal);

		List<Row> toReturn = new ArrayList<Row>();
		for(int i=0; i<geolocalizaciones.size(); i++){
			String geohash = geolocalizaciones.get(i).getString(0);
			mapParameters.put("geohash", geohash);
			mapParameters.put("year", minYear);
			mapParameters.put("month", minMonth);
			mapParameters.put("day", minDay);
			mapParameters.put("searchFlag", "upper");
			toReturn.addAll(bestTasas(ses, mapParameters));
		}
		final int importeDestinoId = importeDestinoIdDefault + 2;
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

	public static List<Row> seleccionarMejoresTasasGeolocalizacionCompetidorRangoFechasRangoImporteNominal(String paisDestino, String lat, String lon, String distancia,
			String divisa, String competidor, 
			String minYear, String minMonth, String minDay,
			String maxYear, String maxMonth, String maxDay,
			String minRangeImporteNominal, String maxRangeImporteNominal){

		Session ses = connect();
		List<Row> geolocalizaciones = seleccionarGeolocalizacionCompetidorDistancia(paisDestino, lat, lon, distancia, divisa, competidor);
		HashMap<String, String> mapParameters = defineMapParameters("geohash_competidor_importe_nominal_query", paisDestino, divisa, minYear, minMonth, minDay);
		mapParameters.put("competidor", competidor);
		mapParameters.put("maxYear", maxYear);
		mapParameters.put("maxMonth", maxMonth);
		mapParameters.put("maxDay", maxDay);
		mapParameters.put("minRangeImporteNominal", minRangeImporteNominal);
		mapParameters.put("maxRangeImporteNominal", maxRangeImporteNominal);

		List<Row> toReturn = new ArrayList<Row>();
		for(int i=0; i<geolocalizaciones.size(); i++){
			String geohash = geolocalizaciones.get(i).getString(0);
			mapParameters.put("geohash", geohash);
			mapParameters.put("year", minYear);
			mapParameters.put("month", minMonth);
			mapParameters.put("day", minDay);
			toReturn.addAll(bestTasas(ses, mapParameters));
		}
		final int importeDestinoId = importeDestinoIdDefault + 2;
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
	private static List<Row> bestTasas(Session ses, HashMap<String, String> mapParameters){

		// Start date in string format.
		String s = mapParameters.get("year")+"-"+mapParameters.get("month")+"-"+mapParameters.get("day");

		// End date in string format.
		String e = s;

		if(mapParameters.get("maxYear") != "" && mapParameters.get("maxMonth") != "" & mapParameters.get("maxDay") != ""){// If end date is defined, change end date.
			e = mapParameters.get("maxYear")+"-"+mapParameters.get("maxMonth")+"-"+mapParameters.get("maxDay");
		}

		// Calculate number of days between start date and end date.
		LocalDate startDate = LocalDate.parse(s);
		LocalDate endDate = LocalDate.parse(e);
		int days = Days.daysBetween(startDate, endDate).getDays();

		List<Row> toReturn = new ArrayList<Row>();
		for (int i=0; i <= days; i++) {
			LocalDate d = startDate.withFieldAdded(DurationFieldType.days(), i);
			mapParameters.put("year",  d.year().getAsString());
			mapParameters.put("month", d.monthOfYear().getAsString());
			mapParameters.put("day",  d.dayOfMonth().getAsString());	

			// Particular case if exact importe_nominal does not exist.
			if(mapParameters.get("searchFlag") == "lower"){
				List<Row> lowerValueList = bestTasaGivenDate(ses, mapParameters);
				double lowerValue = lowerValueList.get(0).getDouble(0);
				String oldImporteNominal = mapParameters.get("importeNominal");

				if(Math.abs(lowerValue) < 10000000.0){	
					mapParameters.put("importeNominal", String.valueOf(Double.parseDouble(mapParameters.get("importeNominal")) - lowerValue));
					mapParameters.put("searchFlag", "");
					toReturn.addAll(bestTasaGivenDate(ses, mapParameters));
				}
				mapParameters.put("searchFlag", "lower");
				mapParameters.put("importeNominal", oldImporteNominal);

			}
			// Particular case if exact importe_nominal does not exist.
			else if(mapParameters.get("searchFlag") == "upper"){
				List<Row> upperValueList = bestTasaGivenDate(ses, mapParameters);	
				double upperValue = upperValueList.get(0).getDouble(0);
				String oldImporteNominal = mapParameters.get("importeNominal");

				if(upperValue < 10000000.0){
					mapParameters.put("importeNominal", String.valueOf(Double.parseDouble(mapParameters.get("importeNominal")) + upperValue));
					mapParameters.put("searchFlag", "");
					toReturn.addAll(bestTasaGivenDate(ses, mapParameters));
				}
				mapParameters.put("searchFlag", "upper");
				mapParameters.put("importeNominal", oldImporteNominal);
			}
			else{
				toReturn.addAll(bestTasaGivenDate(ses, mapParameters)); 
			} 
		}
		return toReturn;
	}

	/*
	 * Match the tasas for a given date.
	 * 
	 * @return list of rows with the matches.
	 */
	private static List<Row> bestTasaGivenDate(Session ses, HashMap<String, String> mapParameters){

		String sel = "* ";
		if(mapParameters.get("searchFlag").equals("lower")){
			sel = "min(nearest_lower_importe("+mapParameters.get("importeNominal")+", importe_nominal))";
		}
		else if(mapParameters.get("searchFlag").equals("upper")){
			sel = "min(nearest_upper_importe("+mapParameters.get("importeNominal")+", importe_nominal))";
		}
		else if(mapParameters.get("maxGeohash") != "" && mapParameters.get("minGeohash") != ""){
			sel = "geohash ";
		}

		String query = "SELECT "+sel+"FROM "+mapParameters.get("tableName")+" ";
		query += "WHERE pais_destino='"+mapParameters.get("paisDestino")+"' ";

		if(mapParameters.get("ciudad") != ""){
			query += "AND ciudad='"+mapParameters.get("ciudad")+"' ";
		}
		else if(mapParameters.get("numAgente") != ""){
			query += "AND num_agente="+mapParameters.get("numAgente")+" ";
		}
		else if(mapParameters.get("geohash") != ""){
			query += "AND geohash='"+mapParameters.get("geohash")+"' ";
		}

		query += "AND divisa='"+mapParameters.get("divisa")+"' ";

		if(mapParameters.get("competidor") != ""){
			query += "AND competidor='"+mapParameters.get("competidor")+"' ";
		}

		if(mapParameters.get("year") != "" & mapParameters.get("month") != "" & mapParameters.get("day") != ""){
			query += "AND year="+mapParameters.get("year")+" AND month="+mapParameters.get("month")+" AND day="+mapParameters.get("day")+" ";
		}

		if(mapParameters.get("importeNominal") != "" && mapParameters.get("searchFlag").equals("")){
			query += "AND importe_nominal="+mapParameters.get("importeNominal")+" ";
		}

		if(mapParameters.get("minRangeImporteNominal") != "" && mapParameters.get("maxRangeImporteNominal") != ""){
			query += "AND importe_nominal >= "+mapParameters.get("minRangeImporteNominal")+" AND importe_nominal <= "+mapParameters.get("maxRangeImporteNominal")+" ";
		}

		if(mapParameters.get("maxGeohash") != "" && mapParameters.get("minGeohash") != ""){
			query += "AND geohash <= '"+mapParameters.get("maxGeohash")+"' AND geohash >= '"+mapParameters.get("minGeohash")+"' ";
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


