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
      
      HashMap<String, String> dataToInsert = new HashMap<String, String>();
      
      String paisDestino = "Colombia";
      String ciudad = "BCN";
      String numAgente = "23";
      String divisa = "Peso";
      String competidor = "Western";
      String timestamp = String.valueOf(new Date().getTime());
      String tasaCambio = "1.69"; 
      String comision = "2";
      String importeDestino = String.valueOf(100*Double.parseDouble(tasaCambio) - Double.parseDouble(comision));
      String importeNominal = "200";
      String day = "13";
      String month = "3";
      String year = "2017";
    // String lat = "41.385064";
    //  String lon = "2.173403";
      String lat = "40.418517";
      String lon = "-3.720835";
      
      dataToInsert.put("pais_destino", paisDestino);
      dataToInsert.put("ciudad", ciudad);
      dataToInsert.put("num_agente", numAgente);
      dataToInsert.put("divisa", divisa);
      dataToInsert.put("competidor", competidor);
      dataToInsert.put("tasa_cambio", tasaCambio);
      dataToInsert.put("comision", comision);
      dataToInsert.put("timestamp", timestamp);
      dataToInsert.put("importe_nominal", importeNominal);
      dataToInsert.put("day",  day);
      dataToInsert.put("month", month);
      dataToInsert.put("year", year);
      dataToInsert.put("importe_destino", importeDestino);
      dataToInsert.put("lat", lat);
      dataToInsert.put("lon", lon);
      
     // CassandraLogic.dropAndCreateKeySpace();
   //   CassandraLogic.dropAndCreateTables();
   //  CassandraLogic.insertIntoAllTables(dataToInsert);
      
      
      ciudad = null;
      competidor = null;
      String distancia = null;
      numAgente = null;
      lat = lon = null;
      String minRangeImporteNominal="20", maxRangeImporteNominal = "400";
      String maxYear=null, maxMonth=null, maxDay=null, searchFlag = null;
      importeNominal = null;
      System.out.println(CassandraLogic.seleccionarMejoresTasas(paisDestino, divisa, competidor, ciudad, numAgente, lat, lon, distancia, importeNominal, minRangeImporteNominal, maxRangeImporteNominal, year, month, day, maxYear, maxMonth, maxDay, searchFlag));
      lat = "40.418517";
      lon = "-3.720835";
      distancia = "100";
      
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
