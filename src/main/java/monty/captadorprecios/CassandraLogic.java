package monty.captadorprecios;


import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;

public class CassandraLogic {

	/*
	 * A contact point addresses a Cassandra cluster.
	 */
	private String contactPoint;
	
	/*
	 * A session holds connections to a Cassandra cluster, allowing it to be queried.
	 */
	private Session session;
	
	/*
	 * Name of the key space.
	 */
	private String keySpaceName;
	
	/*
	 * List of columns names that are string type (the rest will be int by default).
	 */
	private List<String> stringList;

	
	/* Create a Cassandra session based on a contactPoint and the key space name.
	 * 
	 * @param contactPoint, contact point of the Cassandra Cluster
	 * @param keySpaceName, name of the key space.
	 * @param createKeySpace, boolean field in order to create a new key space or to just connect the session.
	 */
	public CassandraLogic(String contactPoint, String keySpaceName, boolean createKeySpace){
		this.contactPoint = contactPoint;
		this.keySpaceName = keySpaceName;

		// Connect the application to the Cassandra cluster.
		Cluster cluster = Cluster.builder().addContactPoint(contactPoint).build();
		// Whether to create the key space if needed, or just connect the session.
		this.session = createKeySpace?cluster.connect():cluster.connect(this.keySpaceName);
	}

	/* Connect the Cassandra cluster to a certain key space.
	 * 
	 * @return new Cassandra logic instance.
	 */
	public CassandraLogic connectKeySpace(){	    
		return new CassandraLogic(this.contactPoint, this.keySpaceName, false);
	}
	
	/* Drop and create new key space.
	 * 
	 * First, it tries to drop the key space and then it creates the key space. 
	 * If the drop fails, it creates directly the key space.
	 * 
	 * @return new Cassandra logic instance.
	 */
	public CassandraLogic dropAndCreateKeySpace(){		
		try{ 
			this.session.execute("Drop KEYSPACE "+this.keySpaceName);
			this.createKeySpace();
		}	    
		catch(InvalidQueryException e){ // The drop fails.
			this.createKeySpace();
		}

		return this.connectKeySpace();
	}

	/* Create new key space.
	 * 
	 * Default parameters: 
	 * 	+ replication strategy: SimpleStrategy
	 * 	+ replication factor: 3
	 */
	private void createKeySpace(){
		String createQuery = "CREATE KEYSPACE "+this.keySpaceName
				+ " WITH replication = {'class': 'SimpleStrategy',"
				+ "'replication_factor' : 3}";	

		this.session.execute(createQuery);
	}

	/* Drop and create table.
	 * 
	 * First it tries to drop the table and then it creates the table.
	 * If the drop fails, it creates directly the table.
	 * 
	 * @param tableName, name of the table.
	 * 
	 */
	public void dropAndCreateTable(String tableName){

		try{
			this.session.execute("Drop TABLE "+tableName);
			this.createTable(tableName);
		}
		catch(InvalidQueryException e){ // The drop fails.
			this.createTable(tableName);
		}
	}
	
	/* Create the pagos table.
	 * TODO: set the actual attributes of the table.
	 *
	 * @param tableName, name of the table
	 * 
	 */
	private void createTable(String tableName){
		
		// Set the String attributes.
		this.stringList = new ArrayList<String>();
		stringList.add("pais");
		stringList.add("codigo_postal");
		stringList.add("pais_destino");
		stringList.add("competidor");
		stringList.add("divisa");
		stringList.add("modo_entrega");
		stringList.add("canal_captacion");
		stringList.add("user");
		
		String query = "CREATE TABLE "+tableName+"("
				+ "pais text, "
				+ "codigo_postal text, "
				+ "pais_destino text, "
				+ "competidor text, "
				+ "divisa text, "
				+ "importe varint, "
				+ "modo_entrega text, "
				+ "canal_captacion text, "
				+ "user text, "
				+ "timestamp varint, "
				+ "comision varint, "
				+ "tasa_cambio varint, "
				+ "PRIMARY KEY (pais, codigo_postal, pais_destino, competidor, divisa, importe, modo_entrega) );";

		this.session.execute(query);
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
	public void insertData(String tableName, String[] columnNames, String[] columnValues){
		String query = "INSERT INTO "+tableName+"(";
		for(int i=0; i<columnNames.length; i++){
			if(i == columnNames.length-1){
				query += columnNames[i]+")";
			}
			else{
				query += columnNames[i]+", ";
			}
		}

		query += " VALUES(";
		for(int i=0; i<columnValues.length; i++){
			if(i == columnValues.length-1){
				if(stringList.contains(columnNames[i]))
					query += "'"+columnValues[i]+"'"+");";
				else
					query += Integer.parseInt(columnValues[i])+");";
					
			}
			else{
				if(stringList.contains(columnNames[i]))
					query += "'"+columnValues[i]+"'"+",";
				else
					query += Integer.parseInt(columnValues[i])+",";
			}
		}
		this.session.execute(query);
	}
	
	public Session getSession(){
		return this.session;
	}
}
