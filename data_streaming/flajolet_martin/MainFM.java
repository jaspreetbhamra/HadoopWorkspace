package flajolet_martin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MainFM {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "flajolet_martin");
		job.setJarByClass(flajolet_martin.MainFM.class);
		job.setMapperClass(flajolet_martin.MapperFM.class);

		job.setReducerClass(flajolet_martin.ReducerFM.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path("/home/hduser/Desktop/FM/input"));
		FileOutputFormat.setOutputPath(job, new Path("/home/hduser/Desktop/FM/output"));

		if (!job.waitForCompletion(true))
			return;
	}

}
