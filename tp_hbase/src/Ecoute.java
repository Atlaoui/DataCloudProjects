import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

@SuppressWarnings("deprecation")
public class Ecoute {
	protected static final String MY_NAMESPACE_NAME = "myNamespace";
	static final TableName MY_TABLE_NAME = TableName.valueOf("ecoute");
	static final String COLUMN_FAMILY = "cf1";
	//static final String FILE_URL = "lastfm_fichier_1";
	static final String FILE_URL = "lastfm_fichier_1";
	private static Collection<ColumnFamilyDescriptor> listCol = new ArrayList<>();
	
	
	public static void main(String[] args) throws IOException, Exception{
		   
	      Configuration config = HBaseConfiguration.create();
	      System.out.println("config ok");
	     // config.set("hbase.zookeeper.quorum", "server1.com,server2.fr");
	      System.out.println("config set ok");
	      Connection connection = ConnectionFactory.createConnection();
	      System.out.println("connection ok");
	      Admin admin = connection.getAdmin();
	      System.out.println("admin ok");
    	  
    	  //si cette dernière n’existe pas déjà dans le namespace par défaut
    	  if (!admin.tableExists(MY_TABLE_NAME)) {
	        System.out.println("Creating Table " + MY_TABLE_NAME.getNameAsString());

	        HTableDescriptor tableEcoute = new HTableDescriptor(MY_TABLE_NAME);
	        tableEcoute.addFamily(new HColumnDescriptor(COLUMN_FAMILY)); 
	       
	        System.out.println("Done");
	        //creation de la table
	        admin.createTable(tableEcoute);
    	  }
	        
	        System.out.println("connection to table");
	        Table table = connection.getTable(MY_TABLE_NAME);
	        System.out.println("connocted to table");
	        BufferedReader br = new BufferedReader(new FileReader(FILE_URL));
	        String line;
	        
	        while ((line = br.readLine()) != null) {
	        	String[] mots = line.split(" ");
	        	System.out.println(mots);
	        	
	        	String rowkey = mots[0] + mots[1];
	        	Result res = table.get(new Get(Bytes.toBytes(rowkey)));
	        	Put put = new Put(Bytes.toBytes(rowkey));
	        	
	        	if(res.isEmpty()) { //rowkey n'existe pas
	        		System.out.println("res is empty");
	        		put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("UserId"), Bytes.toBytes(mots[0]));
	        		put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("TrackId"), Bytes.toBytes(mots[1]));
	        		put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("LocalListening"), Bytes.toBytes(Long.parseLong(mots[2])));
	        		put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("RadioListening"), Bytes.toBytes(Long.parseLong(mots[3])));
	        		put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("Skip"), Bytes.toBytes(Long.parseLong(mots[4])));
	        		
	        		table.put(put);//envoi de la requête sur la table

	        	}else { // s'il existe alors incrémentation des compteurs
	        		System.out.println("res is not empty");
	        		Increment increment = new Increment(Bytes.toBytes(rowkey));	
	        		increment.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("LocalListening"), Long.valueOf(mots[2])); 
	        		increment.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("RadioListening"), Long.valueOf(mots[3])); 
	        		increment.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("Skip"), Long.valueOf(mots[4]));
	        		
	        		table.increment(increment);
	        	}
	        }
	        br.close();  
    	  System.out.println("out");
	}
	
}