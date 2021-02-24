
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

public class NombreVente {
	
	static final String COLUMN_FAMILY = "defaultcf";

	static final TableName TABLE_PRODUIT = TableName.valueOf("produit");
	static final TableName TABLE_VENTE = TableName.valueOf("vente");
	
	static HashMap<String, Integer> venteParCat = new HashMap<String, Integer>(); //map nombre de vente par catégorie
	static HashMap<String, List<String>> catListProd = new HashMap<String, List<String>>(); // liste de produit par catégorie
	
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
	      
	      System.out.println(" >>>>> Table Produit");
	      for(Result resP: resultsProd){
			  String prodP = Bytes.toString(resP.getValue(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("idprod")));
			  String catP = Bytes.toString(resP.getValue(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("categorie")));
			  
			  if(catListProd.containsKey(catP)) {
				 System.out.println("catListProd contient deja la categorie "+catP +" ajout de " + prodP);
				 List<String> listProd = catListProd.get(catP);
				 listProd.add(prodP);
				 catListProd.put(catP, listProd);
			  }else {
				  System.out.println("catListProd ne contient pas cette cat "+catP+" ajout de "+ prodP);
				  venteParCat.put(catP,0);
				  List<String> tmp = new ArrayList<String>();
				  tmp.add(prodP);
				  catListProd.put(catP, tmp);
			  }
	      }
	      System.out.println(" >>>>> FIN Table Produit");
	     
	      
	      
	      System.out.println(" >>>>> Table Vente");
	      for(Result resV: resultsVente){
	    	  String prodV = Bytes.toString(resV.getValue(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("produit")));
    		  for (Map.Entry<String,List<String>> mapentry : catListProd.entrySet()) {
    			  List<String> l = mapentry.getValue();
    			  if(l.contains(prodV)) {
    				  String cat = mapentry.getKey();
    				  int tmp = venteParCat.get(cat) + 1;
    				  System.out.println("prodV contient bien ce produit dans la cat "+cat + " ajout de la valeur "+tmp);
    				  venteParCat.put(cat, tmp);
    				  break;
    			  }
	    	  }
	      }
	      System.out.println(" >>>>> FIN Table Vente");
	      
	      System.out.println("AFFICHAGE NB VENTE PAR CATÉGORIE >>>>  ");
	      //affichage du nb vente par cat 
	      for (Map.Entry<String,Integer> mapentry : venteParCat.entrySet()) {
	    	System.out.println(mapentry.getKey() + " : " + mapentry.getValue());  
	      }
	}
}