package datacloud.hadoop.noodle;


import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Noodle {
	//public static final Log log = LogFactory.getLog(Noodle.class);
	public static class NoodleMapper extends Mapper<LongWritable,Text, Text, Text>{
		private Text val = new Text();
		private String[] splited_value;

		private String get_plage(String h , int min) {
			StringBuilder s = new StringBuilder().append("entre ").append(h).append("h");
			if(min>30)
				s.append("30 et ").append(h).append("h59");
			else
				s.append("00 et ").append(h).append("h29");
			return s.toString();
		}
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			splited_value = value.toString().split("\\s");
			//en recup la date et heurs
			String[] date_h = splited_value[0].split("_");
			//en set la plage
			val.set(get_plage(date_h[3],Integer.parseInt(date_h[4]))+"-"+splited_value[2]);
			System.out.println(val.toString());
			context.write(new Text(date_h[1]),val);		
		}
	}

	public static class NoodleReducer extends Reducer <Text	,Text,Text,Text>{
		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context) 
				throws IOException, InterruptedException {
			System.err.println("reduce : ");
			Integer cpt =0;		
			HashMap<String, Integer> hm = new HashMap<>();
			String mots_max="";
			//0 tranche horaire 1 mots chercher
			String[] splited_value;
			//map des mots les plus rechercher sur une tranche horraire
			HashMap<String, Tuple> map_mots = new HashMap<>();
			//int cpt;
			for(Text t : values) {
				splited_value=t.toString().split("-");
				cpt=0;
				mots_max="";
				String [] list_mots = splited_value[1].split("\\+");
				for(String mots : list_mots) {
					if(hm.containsKey(mots)) {
						hm.replace(mots, hm.get(mots)+1);
						if(cpt<hm.get(mots)) {
							mots_max=mots;
							cpt=hm.get(mots);
						}	
					}else {
						hm.put(mots, 1);
						if (cpt<1) {
							mots_max=mots;
							cpt++;
						}
					}
				}
				if(map_mots.containsKey(splited_value[0])){
					Tuple tuple = map_mots.get(splited_value[0]);
					if(tuple.nb < cpt) {
						map_mots.replace(splited_value[0], new Tuple(mots_max,cpt));
					}
				}else {
					map_mots.put(splited_value[0], new Tuple(mots_max,cpt));
				}		
			}
			StringBuilder s = new StringBuilder();
			for (Entry<String, Tuple> entry : map_mots.entrySet()) {
			    s.append(entry.getKey()).append(" ").append(entry.getValue().mots).append(" ").append(entry.getValue().nb).append('\n');    
			
			}
		
			context.write(key, new Text(s.toString()));
		}
		
		private class Tuple { 
			public String mots; 
			public int nb; 
			public Tuple(String x, int nb) { 
				this.mots = x; 
				this.nb = nb; 
			} 
		} 
	}

	public static void main(String[] args) throws Exception {
		//get les logs
		//PrintStream o = new PrintStream(new File("console.log"));
		//PrintStream outerr = new PrintStream(new File("erreur.log")); 
		//System.setOut(o);
		//System.setErr(outerr);
		Configuration conf = new Configuration();
		conf.setBoolean("mapreduce.map.speculative", false);
		conf.setBoolean("mapreduce.reduce.speculative", false);
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: Noodle <in> <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "Noodle");
		job.setJarByClass(Noodle.class);//permet d'indiquer le jar qui contient l'ensemble des .class du job à partir d'un nom de classe
		job.setMapperClass(NoodleMapper.class); // indique la classe du Mapper
		job.setReducerClass(NoodleReducer.class); // indique la classe du Reducer
		job.setMapOutputKeyClass(Text.class);// indique la classe  de la clé sortie map
		job.setMapOutputValueClass(Text.class);// indique la classe  de la valeur sortie map    
		job.setOutputKeyClass(Text.class);// indique la classe  de la clé de sortie reduce    
		job.setOutputValueClass(Text.class);// indique la classe  de la valeur de sortie reduce
	
		job.setInputFormatClass(TextInputFormat.class); // indique la classe  du format des données d'entrée
		job.setOutputFormatClass(TextOutputFormat.class); // indique la classe  du format des données de sortie
		//job.setPartitionerClass(HashPartitioner.class);// indique la classe du partitionneur
		job.setNumReduceTasks(12);// nombre de tâche de reduce : il est bien sur possible de changer cette valeur (1 par défaut)
		

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));//indique le ou les chemins HDFS d'entrée
		final Path outDir = new Path(otherArgs[1]);//indique le chemin du dossier de sortie
		FileOutputFormat.setOutputPath(job, outDir);
		final FileSystem fs = FileSystem.get(conf);//récupération d'une référence sur le système de fichier HDFS
		if (fs.exists(outDir)) { // test si le dossier de sortie existe
			fs.delete(outDir, true); // on efface le dossier existant, sinon le job ne se lance pas
		}

		System.exit(job.waitForCompletion(true) ? 0 : 1);// soumission de l'application à Yarn
	}

}
