package tp_hbase;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
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
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class LastFM {

    public static void main(String[] args) {

        Configuration conf = HBaseConfiguration.create();

        conf.set("hbase.zookeeper.quorum","localhost");

        String namespace = "default";
        //String nametable = "ecoute";

        try {

            Connection c = ConnectionFactory.createConnection(conf);
            Admin admin = c.getAdmin();
            TableName tableName = TableName.valueOf("ecoute");

            // elle fait quoi cette ligne
            Table table = c.getTable(tableName);

            //HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(nametable));

            //desc.addFamily(new HColumnDescriptor(Bytes.toBytes("cf1")));

            // Creation de la table si elle n'existe pas
            System.out.println(" avant le premier if de creztion de la tableifff " );
            if (!admin.tableExists(tableName) ){
                System.out.println(" jtable n'existe pas je vais la crée la table" );
                HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(namespace+":"+tableName));
                desc.addFamily(new HColumnDescriptor(Bytes.toBytes("cf1")));
                admin.createTable(desc);


            System.out.println(" jje suis ava,t la lecture du fichier" );



            System.out.println(" jje suis apresssss  fichier" );



            //Table table = c.getTable(desc.getTableName());
            System.out.println(" je suis ouuuuuuuuuuuuut" );
            }
            System.out.println(" je suis ouuuuuuuuuuuuut" );

            FileReader fr = new FileReader("lastfm_fichier_1");
            String line;
            BufferedReader br = new BufferedReader(fr);
            while ((line = br.readLine()) != null) {
                String[] data = line.split(" ");
                byte[] rowkey = Bytes.toBytes(data[0] + data[1]);

                Result res = table.get(new Get(rowkey));

                if (res.isEmpty()) {
                    System.out.println(" je suis le iffffffffffffffffffffffffffffffff" );
                    Put put = new Put(rowkey);
                    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("UserId"), Bytes.toBytes(data[0]));
                    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("TrackId"), Bytes.toBytes(data[1]));
                    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("LocalListening"), Bytes.toBytes(Long.parseLong(data[2])));
                    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("RadioListening"), Bytes.toBytes(Long.parseLong(data[3])));
                    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("Skip"), Bytes.toBytes(Long.parseLong(data[4])));
                    table.put(put);
                } else {
                    Increment inc = new Increment(rowkey);
                    inc.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("LocalListening"), Long.parseLong(data[2]));
                    inc.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("RadioListening"), Long.parseLong(data[3]));
                    inc.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("Skip"), Long.parseLong(data[4]));
                    table.increment(inc);
                    System.out.println(" je suis la elseeeeeeeeeeeeewwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwww" );
                }
            }
            br.close();
            c.close();
        } catch (IOException e) {
            System.out.println("je suis le catch");
            e.printStackTrace();
        }
    }
}