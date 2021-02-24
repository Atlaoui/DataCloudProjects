package tp_hbase;
import java.io.BufferedReader;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;						
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;

import org.apache.hadoop.hbase.client.Increment;
public class CreatAndFill {
	// cf1
	//rowkey UserId TrackId LocalListening RadioListening Skip

	protected static final String NAMESPACE_NAME = "TP_Hbase";
	static final TableName TABLE_NAME = TableName.valueOf("ecoute");
	static final byte[] COLUMN_FAMILY_NAME = Bytes.toBytes("cf1");
	static final byte[] USER_ID = Bytes.toBytes("UserId");
	static final byte[] TRACK_ID = Bytes.toBytes("TrackId");
	static final byte[] LOCALLISTENING = Bytes.toBytes("LocalListening");
	static final byte[] RADIOLISTENING = Bytes.toBytes("RadioListening");
	static final byte[] SKIP = Bytes.toBytes("Skip");

	public static void main(final String[] args) throws IOException {
		try (Connection connection = ConnectionFactory.createConnection();
				Admin admin = connection.getAdmin()) {
			
			admin.getClusterMetrics();// a suprimer
			
			createNamespaceAndTable(admin);
			///home/ta/eclipse-workspace/tp_hbase
			
			try (Table table = connection.getTable(TABLE_NAME)) {
				List<String> l = putContentInTable("lastfm_fichier_1",table);

				getAndPrintRowContents(table,l);
				
				printTableWithScan(connection);
				
				removeContentInTable("lastfm_fichier_1",table);
			}
			deleteNamespaceAndTable(admin);
		}
	}
	
	
	
static void printTableWithScan(Connection connection) throws IOException {
	try (Table table = connection.getTable(TABLE_NAME)) {
		Scan scan = new Scan();
		scan.setCaching(20);
		scan.addFamily(COLUMN_FAMILY_NAME);
		ResultScanner scanner = table.getScanner(scan);
		for (Result r = scanner.next(); (r != null); r = scanner.next()) {
			System.out.println("UserId= "+Bytes.toString(r.getValue(COLUMN_FAMILY_NAME, USER_ID)));
			System.out.println("TrackId= "+Bytes.toString(r.getValue(COLUMN_FAMILY_NAME, TRACK_ID)));
			System.out.println("Localistening= "+Bytes.toLong(r.getValue(COLUMN_FAMILY_NAME, LOCALLISTENING)));
			System.out.println("RadioListening= "+Bytes.toLong(r.getValue(COLUMN_FAMILY_NAME, RADIOLISTENING)));
			System.out.println("Skiped= "+Bytes.toLong(r.getValue(COLUMN_FAMILY_NAME, SKIP)));
		}
	}
}
	

	static List<String> putContentInTable(String Path , final Table table) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(Path));
		String line;
		Result row;
		List<String> rowIds = new ArrayList<>();
		while ((line = br.readLine()) != null) {
			String [] elems = line.split(" ");
			String UserId = elems[0];
			String TrackId = elems[1];
			
			Long LocalListening= Long.parseLong(elems[2]);
			Long RadioListening= Long.parseLong(elems[3]);
			Long Skip= Long.parseLong(elems[4]);
			String rowkey= UserId+TrackId;
			rowIds.add(rowkey);
			row = table.get(new Get(Bytes.toBytes(rowkey)));
			if (row.isEmpty()) {
				table.put(new Put(Bytes.toBytes(rowkey))
						.addColumn(COLUMN_FAMILY_NAME,USER_ID,Bytes.toBytes(UserId))
						.addColumn(COLUMN_FAMILY_NAME,TRACK_ID,Bytes.toBytes(TrackId))
						.addColumn(COLUMN_FAMILY_NAME, LOCALLISTENING,Bytes.toBytes(LocalListening))
						.addColumn(COLUMN_FAMILY_NAME, RADIOLISTENING, Bytes.toBytes(RadioListening))
						.addColumn(COLUMN_FAMILY_NAME, SKIP, Bytes.toBytes(Skip)));
			}else {
				Increment increment = new Increment(Bytes.toBytes(rowkey));
				increment.addColumn(COLUMN_FAMILY_NAME, LOCALLISTENING,LocalListening);
				increment.addColumn(COLUMN_FAMILY_NAME, RADIOLISTENING, RadioListening);
				increment.addColumn(COLUMN_FAMILY_NAME, SKIP, Skip);
			}
		}
		br.close();
		return rowIds;
	}



	






	static void removeContentInTable(String Path , final Table table) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(Path));
		String line;		
		while ((line = br.readLine()) != null) {
			String [] elems = line.split(" ");
			String UserId = elems[0];
			String TrackId = elems[1];
			String rowkey= UserId+TrackId;
			table.delete(new Delete(Bytes.toBytes(rowkey)));
		}
		br.close();
	}

	static void deleteNamespaceAndTable(final Admin admin) throws IOException {
		if (admin.tableExists(TABLE_NAME)) {
			admin.disableTable(TABLE_NAME); 
			admin.deleteTable(TABLE_NAME);
		}
		if (namespaceExists(admin, NAMESPACE_NAME)) 
			admin.deleteNamespace(NAMESPACE_NAME);

	}


	static void createNamespaceAndTable(final Admin admin) throws IOException {

		if (!namespaceExists(admin, NAMESPACE_NAME)) {
			admin.createNamespace(NamespaceDescriptor
					.create(NAMESPACE_NAME).build());
		}
		if (!admin.tableExists(TABLE_NAME)) {
			HTableDescriptor table = new HTableDescriptor(TABLE_NAME);
			table.addFamily(new HColumnDescriptor(COLUMN_FAMILY_NAME));
			admin.createTable(table);
		}
	}



	//true si il existe sinon false 
	static boolean namespaceExists(final Admin admin, final String namespaceName)
			throws IOException {
		try {
			admin.getNamespaceDescriptor(namespaceName);
		} catch (NamespaceNotFoundException e) {
			return false;
		}
		return true;
	}
	
	
	static void getAndPrintRowContents(final Table table,List<String> l) throws IOException {
		for(String e : l) {
			Result row = table.get(new Get(Bytes.toBytes(e)));

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

}
