import java.io.IOException;
import java.math.BigInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
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

public class Exo2Agrégat { //nombre de ventes par nom de catégorie

	public static void main(String[] args){

		//connexion à HBase 
		Configuration conf = HBaseConfiguration.create(); 
		conf.set("hbase.zookeeper.quorum", "localhost"); 
		Connection c;

		try {
			c = ConnectionFactory.createConnection(conf);

			long start = java.lang.System.currentTimeMillis() ;

			TableName tableNameVente = TableName.valueOf("vente");
			Table tableVente = c.getTable(tableNameVente); 
			TableName tableNameProd = TableName.valueOf("produit");
			Table tableProduit = c.getTable(tableNameProd); 
			TableName tableNameCatg = TableName.valueOf("categorie");
			Table tableCategorie = c.getTable(tableNameCatg); 

			//récupérer la colonne produit de la table VENTE
			Scan scan = new Scan(); 
			scan.addColumn(Bytes.toBytes("defaultcf"), Bytes.toBytes("produit"));
			ResultScanner results = tableVente.getScanner(scan);

			for(Result res: results){
				String colProduit = Bytes.toString(res.getValue(Bytes.toBytes("defaultcf"), Bytes.toBytes("produit")));
								
				//récupérer dans PRODUIT la ligne avec : rowkey (de PRODUIT) = colProduit  
				Get get = new Get(Bytes.toBytes(colProduit));
				get.addColumn(Bytes.toBytes("defaultcf"), Bytes.toBytes("categorie"));
				Result res1 = tableProduit.get(get);//envoi de la requête sur la table 	
				
				if(res1.isEmpty()) {
					//System.out.print(" res1 is empty !!!!!");
				}else {
					byte[] rowkeyC = res1.getValue(Bytes.toBytes("defaultcf"),Bytes.toBytes("categorie"));

					//mettre à jour le nombre de vente pour la catégorie du produit vendu
					Get get2 = new Get(rowkeyC);
					Result res2 = tableCategorie.get(get2);//envoi de la requête sur la table 

					if (res2.isEmpty()) {
						//initialiser la colonne à 1 (ou 0?)
						Put put = new Put(rowkeyC);
					    BigInteger bigInt = BigInteger.valueOf(1);      
						put.addColumn(Bytes.toBytes("defaultcf"), Bytes.toBytes("nombreVente"),bigInt.toByteArray());
						tableCategorie.put(put);

					}else {

						Increment inc = new Increment(rowkeyC);
						inc.addColumn(Bytes.toBytes("defaultcf"), Bytes.toBytes("nombreVente"), Long.valueOf(1));
						tableCategorie.increment(inc);
					}
				}
			}

			
			long end = java.lang.System.currentTimeMillis() ;
			long temps = end - start ;
			System.out.println("Temps ecoule = " + temps + " ms") ;

			
			Scan scanZ = new Scan(); 
			scanZ.addColumn(Bytes.toBytes("defaultcf"), Bytes.toBytes("idcat"));
			scanZ.addColumn(Bytes.toBytes("defaultcf"), Bytes.toBytes("nombreVente"));
			ResultScanner resultsZ = tableCategorie.getScanner(scan);

			for(Result res: resultsZ){
				String nbV = Bytes.toString(res.getValue(Bytes.toBytes("defaultcf"), Bytes.toBytes("nombreVente")));
				System.out.println("idcat = " + Bytes.toString(res.getRow()) + " nbV = " + nbV );
			}
			
			
			c.close(); 

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}