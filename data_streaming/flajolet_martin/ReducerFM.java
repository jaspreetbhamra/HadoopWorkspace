package flajolet_martin;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerFM extends Reducer<Text, LongWritable, Text, DoubleWritable> {

	long max = 0;
	
	public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		// process values
		for (LongWritable val : values) {
			long no = val.get();
			if(no>max)
				max = no;
		}
		double distinct = Math.pow(2, max);
//		System.out.println("Distint: "+distinct);
		context.write(new Text("The number of distinct elements in the data stream is: "), new DoubleWritable(distinct));
	}

}
