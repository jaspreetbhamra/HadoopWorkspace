package page_rank;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MultiplicationMain {

	public static void main(String[] args) throws Exception {
		
		int maxNumberOfIterations = 10;
		int iterationNumber = 1;
		
		Configuration conf = new Configuration();
		conf.set("row_a", "5");
		conf.set("col_a", "5");
		conf.set("row_b", "5");
		conf.set("col_b", "1");
		
		do {
			Job job = Job.getInstance(conf, "page_rank");
			job.setJarByClass(page_rank.MultiplicationMain.class);
			job.setMapperClass(page_rank.MultiplicationMapper.class);
	
			job.setReducerClass(page_rank.MultiplicationReducer.class);
	
			// TODO: specify output types
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			Path inputPath = new Path("C:\\Users\\JaspreetKaur\\Desktop\\HadoopProject\\HadoopProjects\\page_rank_inputs_outputs\\rank"+(iterationNumber-1)+"\\part-r-00000");
			String inputPaths = "C:\\Users\\JaspreetKaur\\Desktop\\HadoopProject\\HadoopProjects\\page_rank_inputs_outputs\\matrixM,C:\\Users\\JaspreetKaur\\Desktop\\HadoopProject\\HadoopProjects\\page_rank_inputs_outputs\\rank"+(iterationNumber-1)+"\\part-r-00000";
			Path outputPath = new Path("C:\\Users\\JaspreetKaur\\Desktop\\HadoopProject\\HadoopProjects\\page_rank_inputs_outputs\\rank"+iterationNumber);
	
			// TODO: specify input and output DIRECTORIES (not files)
			FileInputFormat.setInputPaths(job, inputPaths);
			FileOutputFormat.setOutputPath(job, outputPath);
			
			if (!job.waitForCompletion(true))
				return;
			
//			if(isEqual(inputPath, outputPath))
//				break;
			
			System.out.println("Iteration "+iterationNumber+" complete..");
			++iterationNumber;
			
		} while (iterationNumber <= maxNumberOfIterations);
	}
	
//	public static boolean isEqual(Path p1, Path p2) throws IOException {
//		BufferedReader br1 = new BufferedReader(new FileReader(p1.getName()));
//		BufferedReader br2 = new BufferedReader(new FileReader(p2.getName()+"\\part-r-00000"));
//		String line1 = null, line2 = null;
//		do {
//			line1 = br1.readLine();
//			line2 = br2.readLine();
//			if(!line1.equals(line2))
//				return false;
//		} while(line1 != null && line2 != null);
//		br1.close();
//		br2.close();
//		return true;
//	}

}
