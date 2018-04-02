package pcy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MainPCY extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		
//		Configuration conf = new Configuration();
//		
//			Job job = Job.getInstance(conf, "pcy");
//			job.setJarByClass(pcy.MainPCY.class);
//			job.setMapperClass(pcy.Pass1Mapper.class);
//	
//			job.setReducerClass(pcy.Pass1Reducer.class);
//	
//			// TODO: specify output types
//			job.setOutputKeyClass(Text.class);
//			job.setOutputValueClass(LongWritable.class);
//			
//			Path inputPath = new Path("C:\\Users\\JaspreetKaur\\Desktop\\HadoopProject\\HadoopProjects\\input");
////			String inputPaths = "C:\\Users\\JaspreetKaur\\Desktop\\HadoopProject\\HadoopProjects\\page_rank_inputs_outputs\\matrixM,C:\\Users\\JaspreetKaur\\Desktop\\HadoopProject\\HadoopProjects\\page_rank_inputs_outputs\\rank"+(iterationNumber-1)+"\\part-r-00000";
//			Path outputPath = new Path("C:\\Users\\JaspreetKaur\\Desktop\\HadoopProject\\HadoopProjects\\output");
//	
//			// TODO: specify input and output DIRECTORIES (not files)
//			FileInputFormat.setInputPaths(job, inputPath);
//			FileOutputFormat.setOutputPath(job, outputPath);
//			
//			if (!job.waitForCompletion(true))
//				return;
//			
		
		int exitCode = ToolRunner.run(new MainPCY(), args);  
		System.exit(exitCode);
			
	}

	@Override
	public int run(String[] args) throws Exception {
		JobControl jobControl = new JobControl("jobChain");
		
		Configuration config1 = getConf();
		Job job1 = Job.getInstance(config1);
		job1.setJarByClass(pcy.MainPCY.class);
		job1.setJobName("Calculation of initial support values");
		FileInputFormat.setInputPaths(job1, new Path("C:\\Users\\JaspreetKaur\\Desktop\\HadoopProject\\HadoopProjects\\input"));
		FileOutputFormat.setOutputPath(job1, new Path("C:\\Users\\JaspreetKaur\\Desktop\\HadoopProject\\HadoopProjects\\output\\temp"));
		job1.setMapperClass(pcy.Pass1Mapper.class);
		job1.setReducerClass(pcy.Pass1Reducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(LongWritable.class);
		ControlledJob controlledJob1 = new ControlledJob(config1);
		controlledJob1.setJob(job1);
		jobControl.addJob(controlledJob1);
		
		Configuration config2 = getConf();
		Job job2 = Job.getInstance(config2);
	    job2.setJarByClass(pcy.MainPCY.class);
	    job2.setJobName("Hashing into Buckets");
	    FileInputFormat.setInputPaths(job2, new Path("C:\\Users\\JaspreetKaur\\Desktop\\HadoopProject\\HadoopProjects\\output\\temp"));
	    FileOutputFormat.setOutputPath(job2, new Path("C:\\Users\\JaspreetKaur\\Desktop\\HadoopProject\\HadoopProjects\\output\\final"));
	    job2.setMapperClass(pcy.Pass2Mapper.class);
	    job2.setReducerClass(pcy.Pass2Reducer.class);
	    job2.setOutputKeyClass(Text.class);
	    job2.setOutputValueClass(LongWritable.class);
	    ControlledJob controlledJob2 = new ControlledJob(config2);
	    controlledJob2.setJob(job2);
	    
	    // make job2 dependent on job1 => job2 cannot start until job 1 has been completed successfully
	    controlledJob2.addDependingJob(controlledJob1); 
	    // add the job to the job control
	    jobControl.addJob(controlledJob2);
	    
	    Thread jobControlThread = new Thread(jobControl);
	    jobControlThread.start();
	    
	    while (!jobControl.allFinished()) {
	        System.out.println("Jobs in waiting state: " + jobControl.getWaitingJobList().size());  
	        System.out.println("Jobs in ready state: " + jobControl.getReadyJobsList().size());
	        System.out.println("Jobs in running state: " + jobControl.getRunningJobList().size());
	        System.out.println("Jobs in success state: " + jobControl.getSuccessfulJobList().size());
	        System.out.println("Jobs in failed state: " + jobControl.getFailedJobList().size());
		    try {
		        Thread.sleep(5000);
		    } catch (Exception e) {
	
		    }
	    } 
	    System.exit(0); 
		return (job1.waitForCompletion(true) ? 0 : 1);
	}

}
