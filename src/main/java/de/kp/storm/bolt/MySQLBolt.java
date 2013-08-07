package de.kp.storm.bolt;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

import de.kp.storm.util.ColumnInfo;
import de.kp.storm.util.TableInfo;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class MySQLBolt extends BaseBasicBolt{

	private static final long serialVersionUID = -4664675533134119608L;
	
	private static String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
	private Connection connection;
	
	private PreparedStatement insertStatement;
	private TableInfo tableInfo = TableInfo.getInstance();
	
	private AtomicInteger counter = new AtomicInteger(0);
	private int batchsize;
	
	/* (non-Javadoc)
	 * @see backtype.storm.topology.IBasicBolt#execute(backtype.storm.tuple.Tuple, backtype.storm.topology.BasicOutputCollector)
	 */
	public void execute(Tuple input, BasicOutputCollector collector) {
	    
	    if (input != null) {
	    	
	       List<Object> inputTupleList = (List<Object>) input.getValues();

	       int index = 0;
	       int card = tableInfo.size();
	       
	       /*
	        * An input tuple is a single database row
	        */
	       for (int i=0; i < card; i++) {
	    	   
	           ColumnInfo column = tableInfo.getColumn(i);
	           try {
	        	   
	               index = i + 1;
	               if (column.getType().equalsIgnoreCase("String"))             
	                   insertStatement.setString(index, inputTupleList.get(i).toString());

	               else if (column.getType().equalsIgnoreCase("int"))
	                   insertStatement.setInt(index, Integer.parseInt(inputTupleList.get(i).toString()));
	               
	               else if (column.getType().equalsIgnoreCase("long"))
	                   insertStatement.setLong(index, Long.parseLong(inputTupleList.get(i).toString()));
	               
	               else if (column.getType().equalsIgnoreCase("float")) 
	                   insertStatement.setFloat(index, Float.parseFloat(inputTupleList.get(i).toString()));
	               
	               else if (column.getType().equalsIgnoreCase("double"))
	                   insertStatement.setDouble(index, Double.parseDouble(inputTupleList.get(i).toString()));
	               
	               else if (column.getType().equalsIgnoreCase("short"))
	                   insertStatement.setShort(index, Short.parseShort(inputTupleList.get(i).toString()));
	               
	               else if (column.getType().equalsIgnoreCase("boolean"))
	                   insertStatement.setBoolean(index, Boolean.parseBoolean(inputTupleList.get(i).toString()));
	               
	               else if (column.getType().equalsIgnoreCase("byte"))
	                   insertStatement.setByte(index, Byte.parseByte(inputTupleList.get(i).toString()));
	               
	               else if (column.getType().equalsIgnoreCase("Date")) {

	            	   Date dateToAdd=null;
	            	   if (!(inputTupleList.get(i) instanceof Date)) {  
	                       
	            		   DateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
	                       try {
	                           dateToAdd = (Date)df.parse(inputTupleList.get(i).toString());
	                       
	                       } catch (Exception e) {
	                           e.printStackTrace();
	                       }
	                   
	            	   } else {
	            
	            		   dateToAdd = (Date)inputTupleList.get(i);
	            		   
	            		   Date sqlDate = new Date(dateToAdd.getTime());
	            		   insertStatement.setDate(index, sqlDate);
	            	   
	            	   }   
	            
	               }  
	               
	           } catch (Exception e) {
	             e.printStackTrace();
	           }
	       }

	       try {
	    	   
	    	   insertStatement.addBatch();
	    	   counter.incrementAndGet();
	    	   
	    	   if (counter.get() == batchsize) executeBatch();
	    
	       } catch (Exception e) {
	    	   e.printStackTrace();
	       }           
	   
	    } else {
	    	/*
	    	 * no input
	    	 */
	    	try {
                executeBatch();

	    	} catch (Exception e) {
                 e.printStackTrace();
            }

	    }

	}

	/* (non-Javadoc)
	 * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
	 */
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}
	
	/* (non-Javadoc)
	 * @see backtype.storm.topology.base.BaseBasicBolt#prepare(java.util.Map, backtype.storm.task.TopologyContext)
	 */
	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context) {       
	    
		try {
			
			/*
			 * credentials
			 */
			String user = (String)conf.get("user");
			String pass = (String)conf.get("pass");
			
	        Class.forName(MYSQL_DRIVER);
	        String endpoint = getEndpoint(conf);
    		connection = DriverManager.getConnection(endpoint, user, pass);

    		/*
    		 * table
    		 */
    		String table = (String)conf.get("table");   		
    		connection.prepareStatement("DROP TABLE IF EXISTS " + table).execute();
  	       
    		ColumnInfo column = null;
    		int card = tableInfo.size();
    		/*
    		 * create query
    		 */
 	       StringBuilder cquery = new StringBuilder("CREATE TABLE IF NOT EXISTS " + table +"(");
 	       
 	       column = tableInfo.getColumn(0);
 	       cquery.append(column.toString());
 	       
 	       if (card > 0) {
 	    	   for (int c=1; c < card; c++) {
 	    		   cquery.append("," + tableInfo.getColumn(c).toString());
 	    	   }
 	       }
	       
	       connection.prepareStatement(cquery.toString()).execute();

	       /*
	        * prepare insert query
	        */
	       StringBuilder iquery = new StringBuilder("INSERT INTO " + table +"(");
	       
	       column = tableInfo.getColumn(0);
	       cquery.append(column.getName());
	       
	       if (card > 0) {
	    	   for (int c=1; c < card; c++) {
	    		   cquery.append("," + tableInfo.getColumn(c).getName());
	    	   }
	       }
	       
	       iquery.append(") values (?");
	       
	       if (card > 0) {
	    	   for (int c=1; c < card; c++) {
	    		   iquery.append(",?");
	    	   }
	       }
	 
	       iquery.append("?)");
	       insertStatement = connection.prepareStatement(iquery.toString());
	       
	       /*
	        * batch size
	        */
	       String batchsizeStr = (String)conf.get("batchsize");
	       batchsize = (batchsizeStr == null) ? 100 : Integer.valueOf(batchsizeStr);
	       
		} catch (Exception e) {
	        e.printStackTrace();

		}

	}

	public void executeBatch() throws Exception {

		insertStatement.executeBatch();	
	    counter = new AtomicInteger(0);
	
	}
	
	private String getEndpoint(@SuppressWarnings("rawtypes") Map conf) {

		String host = (String)conf.get("host");
		String port = (String)conf.get("port");
		
		String database = (String)conf.get("dbase");
        String endpoint = "jdbc:mysql://" + host + ":" + port+"/" + database;
        
        return endpoint;
        
	}
}
