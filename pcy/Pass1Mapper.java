package pcy;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Pass1Mapper extends Mapper<LongWritable, Text, Text, LongWritable>{
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String arr[] = value.toString().split(",");
		for(int i = 0; i < arr.length; ++i) {
			for(int j = i+1; j < arr.length; ++j) {
				context.write(new Text(arr[i]+","+arr[j]), new LongWritable(1));
			}
		}
	}

}
