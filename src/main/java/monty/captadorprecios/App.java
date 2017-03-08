package monty.captadorprecios;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

public class App {

   public static void main(String args[]) throws Exception{
    
	  // Define contact point and keyspace name.
	   String contactPoint = "127.0.0.1";
	   String keyspaceName = "captadorprecios";
      
      // Creating a CassandraLogic object
      CassandraLogic cassandraKeySpace = new CassandraLogic(contactPoint, keyspaceName, true);
      
      // Create and drop the key space.
      CassandraLogic cassandra = cassandraKeySpace.dropAndCreateKeySpace();
      // Just connect to the key space.
      //CassandraLogic cassandra = cassandraKeySpace.connectKeySpace();
      
      // Create and drop the table. Comment if you do not want to reset the table.
      cassandra.dropAndCreateTable("precios");
     
      // Include mock data.
      String[] columnNames = {"pais", "codigo_postal", "pais_destino", "competidor", "divisa", "importe", "modo_entrega", 
    		  "canal_captacion", "user", "timestamp", "comision", "tasa_cambio"};
      String[] columnValues = {"Colombia", "28700", "Mexico", "Western Union", "dolar", "150", "Ventanilla", "Movil", "Javier", "100", "1000", "20"};
      String[] columnValues2 = {"Ecuador", "34500", "Mexico","Paypal",  "dolar", "1000", "Ventanilla", "Lucca", "Enrique", "200", "2000", "2"};
      
      cassandra.insertData("precios", columnNames, columnValues);
      cassandra.insertData("precios", columnNames, columnValues2);
   }
}
