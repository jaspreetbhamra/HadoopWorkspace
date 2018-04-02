package pcy;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Pass1Reducer extends Reducer<Text, LongWritable, Text, LongWritable> {

	public void reduce(Text _key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		long support = 0;
		for(LongWritable val: values) {
			support++;
		}
		context.write(_key, new LongWritable(support));
	}
	
}
