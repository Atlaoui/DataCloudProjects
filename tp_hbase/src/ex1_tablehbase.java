

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import java.util.Map.Entry;
import java.util.NavigableMap;

public class ex1_tablehbase {

	public static void main(String[] args) {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum" , "localhost");
		Connection c;
		try {
			c = ConnectionFactory.createConnection(conf);

			Admin admin = c.getAdmin();

			TableName TABLE_NAME = TableName.valueOf("default"+":"+"ecoute");


			if (admin.tableExists(TABLE_NAME)) {
				admin.disableTable(TABLE_NAME);
				admin.deleteTable(TABLE_NAME);
			}
			TableDescriptorBuilder tbd = TableDescriptorBuilder.newBuilder(TABLE_NAME);
			//The type HTableDescriptor is deprecated


			ColumnFamilyDescriptor cf1 = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("cf1")).build();
			//The type HColumnDescriptor is deprecated
			tbd.setColumnFamily(cf1);

			admin.createTable(tbd.build());

			Table table =  c.getTable(TableName.valueOf("ecoute")) ; //connexion à une table

			BufferedReader br = new BufferedReader(new FileReader("lastfm_fichier_1")); 	//lecture des lignes du fichier généré
			String line;
			while ((line = br.readLine()) != null) { 

				String[] ligne = line.split(" ");
				
				byte[] rowkey = Bytes.toBytes(ligne[0] + ligne[1]); // le rowkey est une concaténation du UserId et du TrackId
				Result res = table.get(new Get(rowkey));

				Put put = new Put(rowkey);

				Long LocalListening = Long.valueOf(ligne[2]);
				Long RadioListening = Long.valueOf(ligne[3]);
				Long Skip = Long.valueOf(ligne[4]);
				
				if (res.isEmpty()) { //on vérifie que la clé n'existe pas

					put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("UserID"), Bytes.toBytes(ligne[0])); //ajout da la 1ere valeur de la ligne dans la cellule <rowkey,cf1:UserID>
					put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("TrackId"), Bytes.toBytes(ligne[1]));
					put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("LocalListening"), Bytes.toBytes(LocalListening));
					put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("RadioListening"), Bytes.toBytes(RadioListening));
					put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("Skip"), Bytes.toBytes(Skip));
					table.put(put); //envoie de la requete sur la table
					
				} else { //le cas où la clé existe déjà
										
					Increment inc = new Increment(rowkey);
					inc.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("LocalListening"), LocalListening);
					inc.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("RadioListening"), RadioListening);
					inc.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("Skip"), Skip);
					table.increment(inc);

				}
				getAndPrintRowContents(table, rowkey);

			} 

			br.close();
			c.close (); 
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//... // code client Hbase
		// fermeture connexion

	}

	
	static void getAndPrintRowContents(final Table table, byte[] rowid) throws IOException {

	    Result row = table.get(new Get(rowid));

	    System.out.println("Row [" + Bytes.toString(row.getRow())
	            + "] was retrieved from Table ["
	            + table.getName().getNameAsString()
	            + "] in HBase, with the following content:");

	    for (Entry<byte[], NavigableMap<byte[], byte[]>> colFamilyEntry
	            : row.getNoVersionMap().entrySet()) {
	      String columnFamilyName = Bytes.toString(colFamilyEntry.getKey());

	      System.out.println("  Columns in Column Family [" + columnFamilyName
	              + "]:");

	      for (Entry<byte[], byte[]> columnNameAndValueMap
	              : colFamilyEntry.getValue().entrySet()) {

	        System.out.println("    Value of Column [" + columnFamilyName + ":"
	                + Bytes.toString(columnNameAndValueMap.getKey()) + "] == "
	                + Bytes.toString(columnNameAndValueMap.getValue()));
	      }
	    }
	  }
}
