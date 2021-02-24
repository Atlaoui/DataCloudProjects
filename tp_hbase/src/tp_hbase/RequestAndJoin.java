package tp_hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Map.Entry;

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

public class RequestAndJoin {
	//http://infocenter.sybase.com/help/index.jsp?topic=/com.sybase.infocenter.dc31014.1653/doc/html/rad1232020345198.html
	//http://www.thecloudavenue.com/2012/02/getting-started-with-hbase.html

	static final TableName TABLE_VENTE = TableName.valueOf("vente");
	static final TableName TABLE_CATEGORIE = TableName.valueOf("categorie");
	static final TableName TABLE_PRODUIT = TableName.valueOf("produit");
	static final TableName TABLE_CLIENT = TableName.valueOf("client");
	static final TableName TABLE_MAGASIN = TableName.valueOf("magasin");
	static final byte[] COLUMN_FAMILY_NAME = Bytes.toBytes("defaultcf");

	//pour la normalisation
	static final TableName TABLE_VENTE_CATEGORIE = TableName.valueOf("vente_categorie");

	static final byte[] PROD_ID = Bytes.toBytes("prodId");
	static final byte[] CAT_ID = Bytes.toBytes("categorieId");

	public static void main(final String[] args) throws IOException, InterruptedException {
		try (Connection connection = ConnectionFactory.createConnection();
				Admin admin = connection.getAdmin()) {

			admin.getClusterMetrics();// a suprimer

			long startTime = System.nanoTime();
			//liste des nom des catégorie si besoin
			Map<String,String> mapNames = getNames(connection);
			System.out.println();
			System.out.println();
			System.out.println("result map en fesant la jointure simple : ");
			System.out.println(join(connection));
			long endTime   = System.nanoTime();
			long totalTime = endTime - startTime;
			System.out.println("Temps d'execution du Join sans normalisation = " +totalTime + " millieseconds");
			System.out.println();
			System.out.println();
			
			//crée une table qui fusionne les colone produit/catégorie/vente
			dnormlizeDataBase(connection);

			Thread.sleep(500);
			
			dnormlizeDataBase_2(connection);

			Thread.sleep(500);
			startTime = System.nanoTime();
			System.out.println("map result Normalization 1 : ");
			System.out.println(getVenteParCategorieNormlized(connection));
			endTime   = System.nanoTime();
			totalTime = endTime - startTime;
			System.out.println("Temps d'execution avec normalisation 1 = " +totalTime + " millieseconds");

			Thread.sleep(100);
			
			startTime = System.nanoTime();
			System.out.println("map result Normalization 2 : ");
			System.out.println(getVenteParCategorieNormlized_2(connection));
			endTime   = System.nanoTime();
			totalTime = endTime - startTime;
			System.out.println("Temps d'execution avec normalisation 2 = " +totalTime + " millieseconds");
			System.out.println();
			System.out.println();

		}

	}

	//------------------------------------------------------------------------------------------------------------------------------------------
	//------------------------------------------------   Normalisation Methode 1             ----------------------------------------------------------------------

	//normalisation en ajoutant une colum a la table categoorie 
	static void dnormlizeDataBase(Connection connection) throws IOException{
		Map<String,Integer> table = countVente(getCategorisProduit(connection),connection);	
		try (Table tableCat = connection.getTable(TABLE_CATEGORIE)) {
			for(Entry<String, Integer > e : table.entrySet() ) {
				byte[] key = Bytes.toBytes(e.getKey());
				Get get = new Get(key);
				Result res = tableCat.get(get);
				if( res.getValue(Bytes.toBytes("defaultcf"), Bytes.toBytes("nb_vente")) == null ){
					Increment inc = new Increment(key);
					inc.addColumn(Bytes.toBytes("defaultcf"), Bytes.toBytes("nb_vente"), Long.valueOf(e.getValue()));
					tableCat.increment(inc);
				}
			}
		}
	}

	// get vente par categorie avec un accés juste a la table categorie
	static Map<String,Long> getVenteParCategorieNormlized(Connection connection)  throws IOException{
		Map<String,Long> mapRet = new HashMap<>();
		try (Table table = connection.getTable(TABLE_CATEGORIE)) {
			Scan scan = new Scan();
			scan.setCaching(20);
			scan.addFamily(Bytes.toBytes("defaultcf"));
			ResultScanner scanner = table.getScanner(scan);
			for (Result r = scanner.next(); (r != null); r = scanner.next()) {
				Long nb_vente = Bytes.toLong(r.getValue(Bytes.toBytes("defaultcf"), Bytes.toBytes("nb_vente")));
				String categorie = myGetValue(r,"idcat");
				mapRet.put(categorie, nb_vente);
			}
		}
		return mapRet;
	}

	//------------------------------------------------------------------------------------------------------------------------------------------
	//------------------------------------------------   Normalisation Methode  2           ----------------------------------------------------------------------

	static Map<String,Integer> getVenteParCategorieNormlized_2(Connection connection)  throws IOException{
		Map<String,Integer> mapRet = new HashMap<>();
		try (Table table = connection.getTable(TABLE_VENTE_CATEGORIE)) {
			Scan scan = new Scan();
			scan.setCaching(20);
			scan.addFamily(Bytes.toBytes("defaultcf"));
			ResultScanner scanner = table.getScanner(scan);
			for (Result r = scanner.next(); (r != null); r = scanner.next()) {
				String categorie = myGetValue(r,"categorieId");
				if(mapRet.containsKey(categorie)) {
					Integer cpt = mapRet.get(categorie);
					cpt++;
					mapRet.put(categorie, cpt);
				}else {
					mapRet.put(categorie, 1);
				}
			}
		}
		return mapRet;
	}



	//Ont normalise maison la BD en ajoutant un petite table 
	static void dnormlizeDataBase_2(Connection connection) throws IOException{
		//pour chaque catégorie la list des produit qui en font partie
		Map<String,List<String>> mapCatProduit =  getCategorisProduit(connection);

		//map qui va centenir pour chaque vente un tuple de catéggorie et le produit
		Map<String,Tuple<String>> mapRet = new HashMap<>();

		// en parcour la table des vente en lie chaque vente a chaque produit dans matRet
		try (Table table = connection.getTable(TABLE_VENTE)) {
			Scan scan = new Scan();
			scan.setCaching(20);
			scan.addFamily(Bytes.toBytes("defaultcf"));
			ResultScanner scanner = table.getScanner(scan);
			for (Result r = scanner.next(); (r != null); r = scanner.next()) {
				String produit = myGetValue(r,"produit");
				String idvente = myGetValue(r,"idvente");
				for(Entry<String, List<String>> e : mapCatProduit.entrySet()) {
					//si cette categorie enregistre un vente
					if (e.getValue().contains(produit)) {
						mapRet.put(idvente, new Tuple<String>(e.getKey(),produit));
						break;
					}
				}
			}
		}


		//la on crée la base de données
		try(Admin admin = connection.getAdmin()){
			admin.getClusterMetrics();
			if (!admin.tableExists(TABLE_VENTE_CATEGORIE)) {
				HTableDescriptor table = new HTableDescriptor(TABLE_VENTE_CATEGORIE);
				table.addFamily(new HColumnDescriptor(COLUMN_FAMILY_NAME));
				admin.createTable(table);
			}
		}

		//ont la remplie
		try (Table table = connection.getTable(TABLE_VENTE_CATEGORIE)) {
			for(Entry<String, Tuple<String>> e : mapRet.entrySet()) {
				//pour lancé des teste a la suite sans trop changer le code
				Result row = table.get(new Get(Bytes.toBytes(e.getKey())));
				if (row.isEmpty()) {
					table.put(new Put(Bytes.toBytes(e.getKey()))
							.addColumn(COLUMN_FAMILY_NAME,PROD_ID,Bytes.toBytes(e.getValue().prod))
							.addColumn(COLUMN_FAMILY_NAME,CAT_ID,Bytes.toBytes(e.getValue().cat)));
				}
			}
		}
	}






	//------------------------------------------------------------------------------------------------------------------------------------------
	//------------------------------------------------   Code Jointure           ----------------------------------------------------------------------


	//affiche le nombre de ventes par nom de catégorie
	static Map<String,Integer> join(Connection connection) throws IOException {
		// en prend pour chaque catégorie en mets c'est produit
		Map<String,List<String>> map =  getCategorisProduit(connection);
		//en compte les vente
		Map<String,Integer> mapRes = countVente(map , connection );
		return mapRes;
	}



	//ont récuper tous les magasin et leur ID
	static Map<String,String> getNames(Connection connection) throws IOException {
		Map<String,String> map = new HashMap<>();
		try (Table table = connection.getTable(TABLE_CATEGORIE)) {
			Scan scan = new Scan();
			scan.setCaching(20);
			scan.addFamily(Bytes.toBytes("defaultcf"));
			ResultScanner scanner = table.getScanner(scan);
			for (Result result = scanner.next(); (result != null); result = scanner.next()) {
				Entry<byte[], NavigableMap<byte[], byte[]>> colFamilyEntry = result.getNoVersionMap().firstEntry();
				String categoName =  Bytes.toString(colFamilyEntry.getValue().firstEntry().getValue());
				String  categoID =  Bytes.toString(colFamilyEntry.getValue().lastEntry().getValue());
				map.put(categoID,categoName);		 
			}
		}
		return map;
	}


	static Map<String,List<String>> getCategorisProduit(Connection connection) throws IOException {
		Map<String,List<String>> map = new HashMap<>();
		try (Table table = connection.getTable(TABLE_PRODUIT)) {
			Scan scan = new Scan();
			scan.setCaching(20);
			scan.addFamily(Bytes.toBytes("defaultcf"));
			ResultScanner scanner = table.getScanner(scan);
			for (Result r = scanner.next(); (r != null); r = scanner.next()) {
				String catego = myGetValue(r,"categorie");
				if(map.containsKey(catego)) {
					List<String> l = map.get(catego);
					if(!l.contains(myGetValue(r,"idprod"))) {
						l.add(myGetValue(r,"idprod"));
						map.put(catego, l);
					}
				}else {
					List<String> l2 = new ArrayList<String>();
					map.put(catego, l2);
				}
			}
		}
		return map;
	}



	//------------------------------------------------------------------------------------------------------------------------------------------
	//------------------------------------------------   Code count vente           ----------------------------------------------------------------------


	static Map<String,Integer> countVente(Map<String,List<String>> mapCatProd ,Connection connection )  throws IOException{
		Map<String,Integer> mapRet = new HashMap<>();
		try (Table table = connection.getTable(TABLE_VENTE)) {
			Scan scan = new Scan();
			scan.setCaching(20);
			scan.addFamily(Bytes.toBytes("defaultcf"));
			ResultScanner scanner = table.getScanner(scan);
			for (Result r = scanner.next(); (r != null); r = scanner.next()) {
				String produit = myGetValue(r,"produit");
				for(Entry<String, List<String>> e : mapCatProd.entrySet()) {
					//si cette categorie enregistre un vente
					if (e.getValue().contains(produit)) {
						if(mapRet.containsKey(e.getKey())) {
							Integer cpt = mapRet.get(e.getKey());
							cpt++;
							mapRet.put(e.getKey(), cpt);
						}else {
							mapRet.put(e.getKey(), 1);
						}
						break;
					}

				}
			}
		}

		return mapRet;
	}




	//

	private static String myGetValue(Result res , String name) {
		return Bytes.toString(res.getValue(Bytes.toBytes("defaultcf"), Bytes.toBytes(name)));
	}
	static class Tuple<T>{
		public final T cat,prod;
		public Tuple(T categorie, T produit) {cat = categorie;prod = produit;}

	}
}
