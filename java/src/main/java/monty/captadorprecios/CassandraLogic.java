package monty.captadorprecios;


import java.util.ArrayList;
import java.util.Arrays;
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

	private static List<String> stringAttributes = new ArrayList<>(
			Arrays.asList("pais_destino", "competidor", "divisa", "ciudad", "geohash"));

	private static List<String> intAttributes = new ArrayList<>(
			Arrays.asList("num_agente", "day", "month", "year"));


	public static Session connect(){
		Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").withPort(9042).build();
		return cluster.connect("precios_competencia");

	}

	public static Session createConnection(){
		Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").withPort(9042).build();
		return cluster.connect();

	}


	/* Drop and create new key space.
	 * 
	 * First, it tries to drop the key space and then it creates the key space. 
	 * If the drop fails, it creates directly the key space.
	 * 
	 */
	public static void dropAndCreateKeySpace(){
		String keySpaceName = "precios_competencia";
		Session ses = createConnection();
		ses.execute( nearestLowerImporteFunction());
		ses.execute( nearestUpperImporteFunction());

		try{ 
			ses.execute("Drop KEYSPACE "+keySpaceName);
			createKeySpace(ses, keySpaceName);
		}	    
		catch(InvalidQueryException e){ // The drop fails.
			createKeySpace(ses, keySpaceName);
		}
	}

	/* Create new key space.
	 * 
	 * Default parameters: 
	 * 	+ replication strategy: SimpleStrategy
	 * 	+ replication factor: 3
	 */
	private static void createKeySpace(Session ses, String keySpaceName){
		String createQuery = "CREATE KEYSPACE "+ keySpaceName
				+ " WITH replication = {'class': 'SimpleStrategy',"
				+ "'replication_factor' : 3}";	

		ses.execute(createQuery);
	}


	/* Drop and create table.
	 * 
	 * First it tries to drop the table and then it creates the table.
	 * If the drop fails, it creates directly the table.
	 * 
	 * @param tableName, name of the table.
	 * 
	 */
	public static void dropAndCreateTables(){
		Session ses = connect();
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

	public static void insertIntoAllTables(HashMap<String, String> data){
		Session ses = connect();
		HashMap<String, String> tables = defineTables();
		for(String tableName: tables.keySet()){
			if(tableName.startsWith("geohash")){
				data.put("geohash", GeoHash.geoHashStringWithCharacterPrecision(
						Double.parseDouble(data.get("lat")), 
						Double.parseDouble(data.get("lon")), 
						12));			
				insertData(ses, tableName, data);
				data.remove("geohash");
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
	 * @param columnNames, name of the columns to insert
	 * @param columnValues, values of the columns to insert
	 */
	public static void insertData(Session ses, String tableName, HashMap<String, String> data){
		boolean insertFlag = true;

		if(tableName.endsWith("scheme")){
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
			query = query.substring(0, query.length()-1);
			query += ")";
			query += " VALUES(";

			for(String columnName: data.keySet()){
				if( stringAttributes.contains(columnName))
					query += "'"+data.get(columnName)+"'"+",";
				else if( intAttributes.contains(columnName))
					query += Integer.parseInt(data.get(columnName))+",";
				else
					query += Double.parseDouble(data.get(columnName))+",";
			}
			query = query.substring(0, query.length()-1);
			query += ");";	
			ses.execute(query);
		}
	}

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


	public static List<Row> seleccionarMejoresTasasPaisFechaConcreta(String paisDestino, String divisa, String year, String month, String day){

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("pais_query", paisDestino, divisa, year, month, day);		
		return bestTasas(ses, mapParameters);
	}

	public static List<Row> seleccionarMejoresTasasPaisFechaConcretaImporteNominalExacto(String paisDestino, String divisa, String year, 
			String month, String day, String importeNominal){

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("pais_importe_nominal_query", paisDestino, divisa, year, month, day);	
		mapParameters.put("importeNominal", importeNominal);

		return bestTasas(ses, mapParameters);
	}

	public static List<Row> seleccionarMejoresTasasPaisFechaConcretaImporteNominalExactoNoExisteLower(String paisDestino, String divisa, String year, 
			String month, String day, String importeNominal){

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("pais_importe_nominal_query", paisDestino, divisa, year, month, day);	
		mapParameters.put("importeNominal", importeNominal);
		mapParameters.put("searchFlag", "lower");

		return bestTasas(ses, mapParameters);
	}

	public static List<Row> seleccionarMejoresTasasPaisFechaConcretaImporteNominalExactoNoExisteUpper(String paisDestino, String divisa, String year, 
			String month, String day, String importeNominal){

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("pais_importe_nominal_query", paisDestino, divisa, year, month, day);		
		mapParameters.put("importeNominal", importeNominal);
		mapParameters.put("searchFlag", "upper");

		return bestTasas(ses, mapParameters);
	}

	public static List<Row> seleccionarMejoresTasasPaisFechaConcretaRangoImporteNominal(String paisDestino, String divisa, String year, 
			String month, String day, String minRangeImporteNominal, String maxRangeImporteNominal){

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("pais_importe_nominal_query", paisDestino, divisa, year, month, day);	
		mapParameters.put("minRangeImporteNominal", minRangeImporteNominal);
		mapParameters.put("maxRangeImporteNominal", maxRangeImporteNominal);

		return bestTasas(ses, mapParameters);
	}

	public static List<Row> seleccionarMejoresTasasPaisRangoFechas(String paisDestino, String divisa, 
			String minYear, String minMonth, String minDay,
			String maxYear, String maxMonth, String maxDay){

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("pais_importe_nominal_query", paisDestino, divisa, minYear, minMonth, minDay);
		mapParameters.put("maxYear", maxYear);
		mapParameters.put("maxMonth", maxMonth);
		mapParameters.put("maxDay", maxDay);

		return bestTasas(ses, mapParameters);
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

		return bestTasas(ses, mapParameters);
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

		return bestTasas(ses, mapParameters);
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

		return bestTasas(ses, mapParameters);
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

		return bestTasas(ses, mapParameters);
	}



	public static List<Row> seleccionarMejoresTasasCiudadFechaConcreta(String paisDestino, String ciudad, String divisa, String year, String month, String day){

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("ciudad_query", paisDestino, divisa, year, month, day);			
		mapParameters.put("ciudad", ciudad);
		return bestTasas(ses, mapParameters);
	}

	public static List<Row> seleccionarMejoresTasasCiudadFechaConcretaImporteNominalExacto(String paisDestino, String ciudad, String divisa, String year, 
			String month, String day, String importeNominal){

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("ciudad_importe_nominal_query", paisDestino, divisa, year, month, day);	
		mapParameters.put("ciudad", ciudad);
		mapParameters.put("importeNominal", importeNominal);

		return bestTasas(ses, mapParameters);
	}

	public static List<Row> seleccionarMejoresTasasCiudadFechaConcretaImporteNominalExactoNoExisteLower(String paisDestino, String ciudad, String divisa, String year, 
			String month, String day, String importeNominal){

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("ciudad_importe_nominal_query", paisDestino, divisa, year, month, day);	
		mapParameters.put("ciudad", ciudad);
		mapParameters.put("importeNominal", importeNominal);
		mapParameters.put("searchFlag", "lower");

		return bestTasas(ses, mapParameters);
	}

	public static List<Row> seleccionarMejoresTasasCiudadFechaConcretaImporteNominalExactoNoExisteUpper(String paisDestino, String ciudad, String divisa, String year, 
			String month, String day, String importeNominal){

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("ciudad_importe_nominal_query", paisDestino, divisa, year, month, day);	
		mapParameters.put("ciudad", ciudad);
		mapParameters.put("importeNominal", importeNominal);
		mapParameters.put("searchFlag", "upper");

		return bestTasas(ses, mapParameters);
	}

	public static List<Row> seleccionarMejoresTasasCiudadFechaConcretaRangoImporteNominal(String paisDestino, String ciudad, String divisa, String year, 
			String month, String day, String minRangeImporteNominal, String maxRangeImporteNominal){

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("ciudad_importe_nominal_query", paisDestino, divisa, year, month, day);	
		mapParameters.put("ciudad", ciudad);
		mapParameters.put("minRangeImporteNominal", minRangeImporteNominal);
		mapParameters.put("maxRangeImporteNominal", maxRangeImporteNominal);

		return bestTasas(ses, mapParameters);
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

		return bestTasas(ses, mapParameters);
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

		return bestTasas(ses, mapParameters);
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

		return bestTasas(ses, mapParameters);
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

		return bestTasas(ses, mapParameters);
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

		return bestTasas(ses, mapParameters);
	}



	public static List<Row> seleccionarMejoresTasasNumeroAgenteFechaConcreta(String paisDestino, String numAgente, String divisa, String year, String month, String day){

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("agente_query", paisDestino, divisa, year, month, day);		
		mapParameters.put("numAgente", numAgente);
		return bestTasas(ses, mapParameters);
	}

	public static List<Row> seleccionarMejoresTasasNumeroAgenteFechaConcretaImporteNominalExacto(String paisDestino, String numAgente, String divisa, String year, 
			String month, String day, String importeNominal){

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("agente_importe_nominal_query", paisDestino, divisa, year, month, day);	
		mapParameters.put("numAgente", numAgente);	
		mapParameters.put("importeNominal", importeNominal);

		return bestTasas(ses, mapParameters);
	}

	public static List<Row> seleccionarMejoresTasasNumeroAgenteFechaConcretaImporteNominalExactoNoExisteLower(String paisDestino, String numAgente, String divisa, String year, 
			String month, String day, String importeNominal){

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("agente_importe_nominal_query", paisDestino, divisa, year, month, day);	
		mapParameters.put("numAgente", numAgente);
		mapParameters.put("importeNominal", importeNominal);
		mapParameters.put("searchFlag", "lower");

		return bestTasas(ses, mapParameters);
	}

	public static List<Row> seleccionarMejoresTasasNumeroAgenteFechaConcretaImporteNominalExactoNoExisteUpper(String paisDestino, String numAgente, String divisa, String year, 
			String month, String day, String importeNominal){

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("agente_importe_nominal_query", paisDestino, divisa, year, month, day);		
		mapParameters.put("numAgente", numAgente);
		mapParameters.put("importeNominal", importeNominal);
		mapParameters.put("searchFlag", "upper");

		return bestTasas(ses, mapParameters);
	}
	
	public static List<Row> seleccionarMejoresTasasNumeroAgenteFechaConcretaRangoImporteNominal(String paisDestino, String numAgente, String divisa, String year, 
			String month, String day, String minRangeImporteNominal, String maxRangeImporteNominal){

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("agente_importe_nominal_query", paisDestino, divisa, year, month, day);	
		mapParameters.put("numAgente", numAgente);
		mapParameters.put("minRangeImporteNominal", minRangeImporteNominal);
		mapParameters.put("maxRangeImporteNominal", maxRangeImporteNominal);

		return bestTasas(ses, mapParameters);
	}

	public static List<Row> seleccionarMejoresTasasNumeroAgenteRangoFechas(String paisDestino, String numAgente, String divisa, 
			String minYear, String minMonth, String minDay,
			String maxYear, String maxMonth, String maxDay){

		Session ses = connect();
		HashMap<String, String> mapParameters = defineMapParameters("agente_importe_nominal_query", paisDestino, divisa, minYear, minMonth, minDay);
		mapParameters.put("numAgente", numAgente);
		mapParameters.put("maxYear", maxYear);
		mapParameters.put("maxMonth", maxMonth);
		mapParameters.put("maxDay", maxDay);

		return bestTasas(ses, mapParameters);
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

		return bestTasas(ses, mapParameters);
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

		return bestTasas(ses, mapParameters);
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

		return bestTasas(ses, mapParameters);
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

		return bestTasas(ses, mapParameters);
	}

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

		List<Row> toReturn = new ArrayList<Row>();
		for(int i=0; i<geolocalizaciones.size(); i++){
			String geohash = geolocalizaciones.get(i).getString(0);

			mapParameters.put("geohash", geohash);
			toReturn.addAll(bestTasas(ses, mapParameters));
		}
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
		return toReturn;
	}



	public static List<Row> seleccionarMejoresTasasNumeroAgenteRangoFechasImporteNominalExacto(String paisDestino, String lat, String lon, String distancia,
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
		return toReturn;
	}

	public static List<Row> bestTasas(Session ses, HashMap<String, String> mapParameters){

		String s = mapParameters.get("year")+"-"+mapParameters.get("month")+"-"+mapParameters.get("day");
		String e = s;

		if(mapParameters.get("maxYear") != "" && mapParameters.get("maxMonth") != "" & mapParameters.get("maxDay") != ""){
			e = mapParameters.get("maxYear")+"-"+mapParameters.get("maxMonth")+"-"+mapParameters.get("maxDay");
		}

		LocalDate startDate = LocalDate.parse(s);
		LocalDate endDate = LocalDate.parse(e);

		List<Row> toReturn = new ArrayList<Row>();
		int days = Days.daysBetween(startDate, endDate).getDays();

		for (int i=0; i <= days; i++) {
			LocalDate d = startDate.withFieldAdded(DurationFieldType.days(), i);
			mapParameters.put("year",  d.year().getAsString());
			mapParameters.put("month", d.monthOfYear().getAsString());
			mapParameters.put("day",  d.dayOfMonth().getAsString());	

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

	public static List<Row> bestTasaGivenDate(Session ses, HashMap<String, String> mapParameters){

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
		tables.put("geohash_query",  geohash());
		tables.put("geohash_competidor_query",  geohashCompetidor());
		tables.put("geohash_importe_nominal_query",  geohashImporteNominal());
		tables.put("geohash_competidor_importe_nominal_query",  geohashCompetidorImporteNominal());
		return tables;
	}

	private static String pais(){
		String tableName = "pais_query";
		return "CREATE TABLE " + tableName + "(" 
		+  attributes 
		+ "PRIMARY KEY (pais_destino, divisa, year, month, day, importe_destino, timestamp) )" 
		+ "WITH CLUSTERING ORDER BY (divisa DESC, year DESC, month DESC, day DESC, importe_destino DESC, timestamp DESC);";
	}


	private static String paisCompetidor(){
		String tableName = "pais_competidor_query";
		return "CREATE TABLE " + tableName + "("
		+  attributes 
		+ "PRIMARY KEY (pais_destino, divisa, competidor, year, month, day, importe_destino, timestamp) )"
		+ "WITH CLUSTERING ORDER BY (divisa DESC, competidor DESC, year DESC, month DESC, day DESC, importe_destino DESC, timestamp DESC);";
	}

	private static String paisImporteNominal(){
		String tableName = "pais_importe_nominal_query";
		return "CREATE TABLE " + tableName + "(" 
		+  attributes 
		+ "PRIMARY KEY (pais_destino, divisa, year, month, day, importe_nominal, importe_destino, timestamp) )"
		+ "WITH CLUSTERING ORDER BY (divisa DESC, year DESC, month DESC, day DESC, importe_nominal DESC, importe_destino DESC, timestamp DESC);";
	}

	private static String paisCompetidorImporteNominal(){
		String tableName = "pais_competidor_importe_nominal_query";
		return "CREATE TABLE " + tableName + "(" 
		+  attributes 
		+ "PRIMARY KEY (pais_destino, divisa, competidor, year, month, day, importe_nominal, importe_destino, timestamp) )" 
		+ "WITH CLUSTERING ORDER BY (divisa DESC, competidor DESC, year DESC, month DESC, day DESC, importe_nominal DESC, importe_destino DESC, timestamp DESC);";
	}

	private static String ciudad(){
		String tableName = "ciudad_query";
		return "CREATE TABLE " + tableName + "(" 
		+  attributes 
		+ "PRIMARY KEY (pais_destino, ciudad, divisa, year, month, day, importe_destino, timestamp) )" 
		+ "WITH CLUSTERING ORDER BY (ciudad DESC, divisa DESC, year DESC, month DESC, day DESC, importe_destino DESC, timestamp DESC);";
	}


	private static String ciudadCompetidor(){
		String tableName = "ciudad_competidor_query";
		return "CREATE TABLE " + tableName + "("
		+  attributes 
		+ "PRIMARY KEY (pais_destino, ciudad, divisa, competidor, year, month, day, importe_destino, timestamp) )"
		+ "WITH CLUSTERING ORDER BY (ciudad DESC, divisa DESC, competidor DESC, year DESC, month DESC, day DESC, importe_destino DESC, timestamp DESC);";
	}

	private static String ciudadImporteNominal(){
		String tableName = "ciudad_importe_nominal_query";
		return "CREATE TABLE " + tableName + "(" 
		+  attributes 
		+ "PRIMARY KEY (pais_destino, ciudad, divisa, year, month, day, importe_nominal, importe_destino, timestamp) )"
		+ "WITH CLUSTERING ORDER BY (ciudad DESC, divisa DESC, year DESC, month DESC, day DESC, importe_nominal DESC, importe_destino DESC, timestamp DESC);";
	}

	private static String ciudadCompetidorImporteNominal(){
		String tableName = "ciudad_competidor_importe_nominal_query";
		return "CREATE TABLE " + tableName + "(" 
		+  attributes 
		+ "PRIMARY KEY (pais_destino, ciudad, divisa, competidor, year, month, day, importe_nominal, importe_destino, timestamp) )" 
		+ "WITH CLUSTERING ORDER BY (ciudad DESC, divisa DESC, competidor DESC, year DESC, month DESC, day DESC, importe_nominal DESC, importe_destino DESC, timestamp DESC);";
	}

	private static String agente(){
		String tableName = "agente_query";
		return "CREATE TABLE " + tableName + "(" 
		+  attributes 
		+ "PRIMARY KEY (pais_destino, num_agente, divisa, year, month, day, importe_destino, timestamp) )" 
		+ "WITH CLUSTERING ORDER BY (num_agente DESC, divisa DESC, year DESC, month DESC, day DESC, importe_destino DESC, timestamp DESC);";
	}


	private static String agenteCompetidor(){
		String tableName = "agente_competidor_query";
		return "CREATE TABLE " + tableName + "("
		+  attributes 
		+ "PRIMARY KEY (pais_destino, num_agente, divisa, competidor, year, month, day, importe_destino, timestamp) )"
		+ "WITH CLUSTERING ORDER BY (num_agente DESC, divisa DESC, competidor DESC, year DESC, month DESC, day DESC, importe_destino DESC, timestamp DESC);";
	}

	private static String agenteImporteNominal(){
		String tableName = "agente_importe_nominal_query";
		return "CREATE TABLE " + tableName + "(" 
		+  attributes 
		+ "PRIMARY KEY (pais_destino, num_agente, divisa, year, month, day, importe_nominal, importe_destino, timestamp) )"
		+ "WITH CLUSTERING ORDER BY (num_agente DESC, divisa DESC, year DESC, month DESC, day DESC, importe_nominal DESC, importe_destino DESC, timestamp DESC);";
	}

	private static String agenteCompetidorImporteNominal(){
		String tableName = "agente_competidor_importe_nominal_query";
		return "CREATE TABLE " + tableName + "(" 
		+  attributes 
		+ "PRIMARY KEY (pais_destino, num_agente, divisa, competidor, year, month, day, importe_nominal, importe_destino, timestamp) )" 
		+ "WITH CLUSTERING ORDER BY (num_agente DESC, divisa DESC, competidor DESC, year DESC, month DESC, day DESC, importe_nominal DESC, importe_destino DESC, timestamp DESC);";
	}

	private static String geohash_scheme(){
		String tableName = "geohash_scheme";
		return "CREATE TABLE " + tableName + "(" 
		+  attributes 
		+ "geohash text, "
		+ "PRIMARY KEY (pais_destino, divisa, geohash, competidor) )" 
		+ "WITH CLUSTERING ORDER BY (divisa DESC, geohash DESC, competidor DESC);";
	}

	private static String geohash(){
		String tableName = "geohash_query";
		return "CREATE TABLE " + tableName + "(" 
		+  attributes 
		+ "geohash text, "
		+ "PRIMARY KEY (pais_destino, geohash, divisa, year, month, day, importe_destino, timestamp) )" 
		+ "WITH CLUSTERING ORDER BY (geohash DESC, divisa DESC, year DESC, month DESC, day DESC, importe_destino DESC, timestamp DESC);";
	}

	private static String geohashCompetidor(){
		String tableName = "geohash_competidor_query";
		return "CREATE TABLE " + tableName + "("
		+  attributes 
		+ "geohash text, "
		+ "PRIMARY KEY (pais_destino, geohash, divisa, competidor, year, month, day, importe_destino, timestamp) )"
		+ "WITH CLUSTERING ORDER BY (geohash DESC, divisa DESC, competidor DESC, year DESC, month DESC, day DESC, importe_destino DESC, timestamp DESC);";
	}

	private static String geohashImporteNominal(){
		String tableName = "geohash_importe_nominal_query";
		return "CREATE TABLE " + tableName + "(" 
		+  attributes 
		+ "geohash text, "
		+ "PRIMARY KEY (pais_destino, geohash, divisa, year, month, day, importe_nominal, importe_destino, timestamp) )"
		+ "WITH CLUSTERING ORDER BY (geohash DESC, divisa DESC, year DESC, month DESC, day DESC, importe_nominal DESC, importe_destino DESC, timestamp DESC);";
	}

	private static String geohashCompetidorImporteNominal(){
		String tableName = "geohash_competidor_importe_nominal_query";
		return "CREATE TABLE " + tableName + "(" 
		+  attributes 
		+ "geohash text, "
		+ "PRIMARY KEY (pais_destino, geohash, divisa, competidor, year, month, day, importe_nominal, importe_destino, timestamp) )" 
		+ "WITH CLUSTERING ORDER BY (geohash DESC, divisa DESC, competidor DESC, year DESC, month DESC, day DESC, importe_nominal DESC, importe_destino DESC, timestamp DESC);";
	}

	private static String nearestLowerImporteFunction(){
		return "CREATE FUNCTION nearest_lower_importe(importe_user double, importe_nominal double) "
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
		return "CREATE FUNCTION nearest_upper_importe(importe_user double, importe_nominal double) "
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


