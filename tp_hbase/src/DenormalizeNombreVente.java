
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class DenormalizeNombreVente {
	
	static final String COLUMN_FAMILY = "defaultcf";

	static final TableName TABLE_PRODUIT = TableName.valueOf("produit");
	static final TableName TABLE_VENTE = TableName.valueOf("vente");
	static HashMap<String, Integer> nbVenteParProduit = new HashMap<String, Integer>();
	//calcul du nb vente par catégorie
	public static void main(String[] args) throws IOException, Exception{
		 Configuration config = HBaseConfiguration.create();
	      Connection connection = ConnectionFactory.createConnection();
	      Admin admin = connection.getAdmin();
	      
	      Table tableVente = connection.getTable(TABLE_VENTE);
	      Table tableProduit = connection.getTable(TABLE_PRODUIT);
	      
	      Scan scanVente = new Scan();
	      Scan scanProduit = new Scan();
	      
	      ResultScanner resultsVente = tableVente.getScanner(scanVente);
	      ResultScanner resultsProd = tableProduit.getScanner(scanProduit);
	      
	      
    	  for(Result resV : resultsVente) {
    		  String prodV = Bytes.toString(resV.getValue(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("produit")));
    		  if(nbVenteParProduit.containsKey(prodV)) {
    			  Integer tmp = nbVenteParProduit.get(prodV) + 1;
    			  nbVenteParProduit.put(prodV, tmp);
    		  }else {
    			  nbVenteParProduit.put(prodV, 1);
    		  }
    	  }
    	  
	      //ajout de la colonne categorie
	      for(Result resP : resultsProd) {
	        Put put = new Put(resP.getRow());
	        String prodP = Bytes.toString(resP.getValue(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("idprod")));
	        if(nbVenteParProduit.containsKey(prodP)) {
				put.addColumn(Bytes.toBytes(COLUMN_FAMILY), 
						Bytes.toBytes("categorie"),
						Bytes.toBytes(nbVenteParProduit.get(prodP)));
				tableProduit.put(put);	 
	        }else {
	        	put.addColumn(Bytes.toBytes(COLUMN_FAMILY), 
						Bytes.toBytes("categorie"),
						Bytes.toBytes(0));
				tableProduit.put(put);	
	        }
	      }
	}
}