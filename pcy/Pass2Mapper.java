package pcy;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Pass2Mapper extends Mapper<LongWritable, Text, Text, LongWritable>{
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String arr[] = value.toString().split("\t");
		//Bucket number, count=1
		context.write(new Text(arr[1].trim()), new LongWritable(1));
	}

}
