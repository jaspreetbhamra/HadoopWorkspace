package matrix_multiplication;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MultiplicationMain {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("row_a", "2");
		conf.set("col_a", "2");
		conf.set("row_b", "2");
		conf.set("col_b", "1");
		Job job = Job.getInstance(conf, "matrix_multiplication");
		job.setJarByClass(matrix_multiplication.MultiplicationMain.class);
		job.setMapperClass(matrix_multiplication.MultiplicationMapper.class);

		job.setReducerClass(matrix_multiplication.MultiplicationReducer.class);

		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path("C:\\Users\\JaspreetKaur\\Desktop\\HadoopProject\\MatrixMultiplication\\input"));
		FileOutputFormat.setOutputPath(job, new Path("C:\\Users\\JaspreetKaur\\Desktop\\HadoopProject\\MatrixMultiplication\\output"));

		if (!job.waitForCompletion(true))
			return;
	}

}
