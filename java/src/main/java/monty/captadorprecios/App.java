package monty.captadorprecios;

import java.util.Date;
import java.util.HashMap;

public class App {

	public static void main(String args[]) throws Exception{

		// Creating a CassandraLogic object
		//CassandraLogic2 cassandraReset = new CassandraLogic2(contactPoint, keyspaceName, true);
		//cassandraReset.dropAndCreateKeySpace();

		//CassandraLogic2 cassandra = new CassandraLogic2(contactPoint, keyspaceName, false);
		// Just connect to the key space.

		// Create and drop the table. Comment if you do not want to reset the table.
		//  cassandra.dropAndCreateTables();


		String paisDestino = "Colombia";
		String ciudad = "BCN";
		Integer numAgente = 23;
		String divisa = "Peso";
		String competidor = "Western";
		Double timestamp = Double.valueOf(new Date().getTime());
		Double tasaCambio = 1.5; 
		Double comision = 2.0;
		Double importeDestino = 100*tasaCambio - comision;
		Integer importeNominal = 200;
		Integer day = 13;
		Integer month = 3;
		Integer year = 2017;
		// String lat = "41.385064";
		//  String lon = "2.173403";
		Double lat = 40.418517;
		Double lon = -3.720835;



		// CassandraLogic.dropAndCreateKeySpace();
		//CassandraLogic.dropAndCreateTables();
		//CassandraLogic.insertIntoAllTables(paisDestino, divisa, competidor, ciudad, numAgente, lat, lon, tasaCambio, comision, importeNominal, importeDestino,	timestamp, year, month, day);

		
      ciudad = null;
      competidor = null;
      Integer distancia = null;
      numAgente = null;
      lat = lon = null;
      Integer minRangeImporteNominal=20, maxRangeImporteNominal = 400;
      Integer maxYear=null, maxMonth=null, maxDay=null;
      String searchFlag = null;
      importeNominal = null;
      System.out.println(CassandraLogic.seleccionarMejoresTasas(paisDestino, divisa, competidor, ciudad, numAgente, lat, lon, distancia, importeNominal, minRangeImporteNominal, maxRangeImporteNominal, year, month, day, maxYear, maxMonth, maxDay, searchFlag));
      lat = 40.418517;
      lon = -3.720835;
      distancia = 100;
 
      System.out.println(CassandraLogic.seleccionarMejoresTasas(paisDestino, divisa, competidor, ciudad, numAgente, lat, lon, distancia, importeNominal, minRangeImporteNominal, maxRangeImporteNominal, year, month, day, maxYear, maxMonth, maxDay, searchFlag));

		//   System.out.println(CassandraLogic.seleccionarMejoresTasasPaisFechaConcreta("Colombia", "Peso", "2017", "3", "13"));
		// System.out.println(cassandra.seleccionarMejoresTasasNumeroAgenteFechaConcretaCompetidor("Colombia", "23", "Peso", "2017", "3", "13", "Monty"));
		//      
		//      System.out.println();
		//      System.out.println(cassandra.seleccionarMejoresTasasNumeroAgenteFechaConcretaImporteNominalExacto("Colombia", "23", "Peso", "2017", "3", "13", "210"));
		//System.out.println(cassandra.seleccionarMejoresTasasNumeroAgenteFechaConcretaImporteNominalExactoNoExisteLower("Colombia", "23", "Peso", "2017", "3", "13", "210"));
		//      System.out.println(cassandra.seleccionarMejoresTasasNumeroAgenteFechaConcretaImporteNominalExactoNoExisteUpper("Colombia", "23", "Peso", "2017", "3", "13", "210"));
		//      
		//System.out.println();
		// System.out.println(CassandraLogic.seleccionarMejoresTasasNumeroAgenteFechaConcretaRangoImporteNominal("Colombia", "23", "Peso", "2017", "3", "13", "190", "210"));
		//  System.out.println(CassandraLogic.seleccionarMejoresTasasNumeroAgenteFechaConcretaRangoImporteNominal("Colombia", "23", "Peso", "2017", "3", "13", "190", "210"));

		//   System.out.println(CassandraLogic.seleccionarMejoresTasasNumeroAgenteRangoFechas("Colombia", "23", "Peso", "2017", "3", "13", "2017", "3", "14"));
		//      
		//      System.out.println();
		//      System.out.println(cassandra.seleccionarMejoresTasasNumeroAgenteRangoFechasImporteNominalExacto("Colombia", "23", "Peso", "2017", "3", "13", "2017", "3", "14", "120"));
		//System.out.println(cassandra.seleccionarMejoresTasasNumeroAgenteRangoFechasImporteNominalExactoNoExisteLower("Colombia", "23", "Peso", "2017", "3", "13", "2017", "3", "15", "120"));
		//      System.out.println(cassandra.seleccionarMejoresTasasNumeroAgenteRangoFechasImporteNominalExactoNoExisteUpper("Colombia", "23", "Peso", "2017", "3", "13", "2017", "3", "15", "120"));
		//      
		//      System.out.println();
		//  System.out.println(CassandraLogic.seleccionarMejoresTasasNumeroAgenteRangoFechasRangoImporteNominal("Colombia", "23", "Peso", "2017", "3", "13", "2017", "3", "15", "80", "300"));
		//      
		// System.out.println(CassandraLogic.seleccionarGeolocalizacionDistancia("Colombia", "40.418517", "-3.720835", "1000", "Peso"));
		// System.out.println(CassandraLogic.seleccionarMejoresTasasGeolocalizacionFechaConcreta("Colombia", "40.418517", "-3.720835", "10", "Peso", "2017", "3", "13"));
		//System.out.println(CassandraLogic.seleccionarMejoresTasasGeolocalizacionCompetidorFechaConcretaImporteNominalExacto("Colombia", "40.418517", "-3.720835", "10000", "Peso", "Western", "2017", "3", "13", "200"));
		// System.out.println(CassandraLogic.seleccionarMejoresTasasGeolocalizacionFechaConcretaImporteNominalExactoNoExisteLower("Colombia", "40.418517", "-3.720835", "10000", "Peso", "2017", "3", "13", "150"));

		//    System.out.println(CassandraLogic.seleccionarMejoresTasasNumeroAgenteCompetidorRangoFechasRangoImporteNominal("Colombia", "23", "Peso", "Western", "2017", "3", "13", "2017", "3", "15", "80", "300"));
	}
}
