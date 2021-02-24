

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class Ex2_jointure {
	public static void main(String[] args) {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum" , "localhost");
		Connection c;
		long startTime = System.nanoTime();
		
		
		
		try {
			c = ConnectionFactory.createConnection(conf);

			Admin admin = c.getAdmin();
			admin.getClusterMetrics();
			TableName tn_vente = TableName.valueOf("vente");
			TableName tn_produit = TableName.valueOf("produit");
			TableName tn_categorie = TableName.valueOf("categorie");

			Table table_vente = c.getTable(tn_vente);
			Table table_produit = c.getTable(tn_produit);
			Table table_categorie = c.getTable(tn_categorie);

			Map<String, Integer> nbVenteParCategorie = new HashMap<>();
			Map<String, Integer> nbVenteParIDCategorie = new HashMap<>();
			Map<String, Integer> nbVenteParProduit = new HashMap<>();


			Scan scan = new Scan();
			scan.addColumn(Bytes.toBytes("defaultcf"), Bytes.toBytes("produit"));
			ResultScanner results_vente = table_vente.getScanner(scan);

			for(Result r : results_vente) {
				String produit = new String(r.getValue(Bytes.toBytes("defaultcf"), Bytes.toBytes("produit")));
				if(!nbVenteParProduit.containsKey(produit)) {
					nbVenteParProduit.put(produit, 1);
				} else {
					nbVenteParProduit.replace(produit, nbVenteParProduit.get(produit) + 1);
				}
			}
			

			Scan scan2 = new Scan();
			scan2.addColumn(Bytes.toBytes("defaultcf"), Bytes.toBytes("idprod"));
			scan2.addColumn(Bytes.toBytes("defaultcf"), Bytes.toBytes("categorie"));
			ResultScanner results_produit = table_produit.getScanner(scan2);

			for(Result r : results_produit) {
				String produit = new String(r.getValue(Bytes.toBytes("defaultcf"), Bytes.toBytes("idprod")));
				String categorie = new String(r.getValue(Bytes.toBytes("defaultcf"), Bytes.toBytes("categorie")));
				if(!nbVenteParCategorie.containsKey(categorie)) {
					nbVenteParCategorie.put(categorie, nbVenteParProduit.get(produit));
				} else {
					nbVenteParCategorie.replace(categorie, nbVenteParCategorie.get(categorie) + nbVenteParProduit.get(produit));
				}
			}
			
			Scan scan3 = new Scan();
			scan3.addColumn(Bytes.toBytes("defaultcf"), Bytes.toBytes("idcat"));
			scan3.addColumn(Bytes.toBytes("defaultcf"), Bytes.toBytes("designation"));
			ResultScanner result_categorie = table_categorie.getScanner(scan3);

			for(Result r : result_categorie) {
				String idcat = new String(r.getValue(Bytes.toBytes("defaultcf"), Bytes.toBytes("idcat")));
				String designation = new String(r.getValue(Bytes.toBytes("defaultcf"), Bytes.toBytes("designation")));
				if(!nbVenteParCategorie.containsKey(idcat)) {
					nbVenteParCategorie.put(designation, nbVenteParIDCategorie.get(idcat));
				}
			}
			
			for (Entry<String, Integer> m : nbVenteParCategorie.entrySet()) {
				System.out.println("categorie" + m.getKey() + " : " + m.getValue() + " produits vendus");
			}

			c.close (); 
			long endTime   = System.nanoTime();
			long totalTime = endTime - startTime;
			System.out.println("Temps d'execution : " + totalTime + " millieseconds");
			//Temps d'execution : 1921727176 millieseconds
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();

		}

	}
}