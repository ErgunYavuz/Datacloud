package datacloud.hadoop.lastfm;

import java.io.IOException;

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

/*
 * data generated by : ./generateTextfile.sh -t lastfm -n 1 -s 100
 */
public class Lastfm2 {

	public static class LastfmMapper
			extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] data = value.toString().split(" ");

			int listening = Integer.parseInt(data[2])
					+ Integer.parseInt(data[3]);

			context.write(new Text(data[1]),
					new Text(listening + " " + data[4]));

		}
	}

	public static class LastfmReducer
			extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			int totalListening = 0;
			int totalSkip = 0;

			for (Text t : values) {
				String[] data = t.toString().split(" ");
				totalListening += Integer.parseInt(data[0]);
				totalSkip += Integer.parseInt(data[1]);
			}
			
			context.write(key, new Text(totalListening + " " + totalSkip));

		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.setBoolean("mapreduce.map.speculative", true);
		conf.setBoolean("mapreduce.reduce.speculative", true);
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "word count");
		// permet d'indiquer le jar qui contient l'ensemble des .class du job à
		// partir d'un nom de classe
		job.setJarByClass(Lastfm2.class);
		// indique la classe du Mapper
		job.setMapperClass(LastfmMapper.class);
		// indique la classe du Reducer
		job.setReducerClass(LastfmReducer.class);
		// indique la classe de la clé sortie map
		job.setMapOutputKeyClass(Text.class);
		// indique la classe de la valeur sortie map
		job.setMapOutputValueClass(Text.class);
		// indique la classe de la clé de sortie reduce
		job.setOutputKeyClass(Text.class);
		// indique la classe de la clé de sortie reduce
		job.setOutputValueClass(Text.class);
		// indique la classe du format des données d'entrée
		job.setInputFormatClass(TextInputFormat.class);
		// indique la classe du format des données de sortie
		job.setOutputFormatClass(TextOutputFormat.class);
		// indique la classe du partitionneur
		// job.setPartitionerClass(NoodlePartitioner.class);
		// nombre de tâche de reduce : il est bien sur possible de changer cette
		// valeur (1 par défaut)
		job.setNumReduceTasks(1);

		// indique le ou les chemins HDFS d'entrée
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// indique le chemin du dossier de sortie
		final Path outDir = new Path(otherArgs[1]);
		// récupération d'une référence sur le système de fichier HDFS
		FileOutputFormat.setOutputPath(job, outDir);
		// test si le dossier de sortie existe
		final FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outDir)) {
			// on efface le dossier existant, sinon le job ne se lance pas
			fs.delete(outDir, true);
		}

		// soumission de l'application à Yarn
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
