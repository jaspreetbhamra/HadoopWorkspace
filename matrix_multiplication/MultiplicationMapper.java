package matrix_multiplication;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MultiplicationMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line[] = value.toString().split(",");
		Configuration conf = context.getConfiguration(); 
		
 		int rowId = Integer.parseInt(line[1].trim());
		int colId = Integer.parseInt(line[2].trim());
 		
		if(line[0].trim().equals("a")) {
			int numberOfColsInB = Integer.parseInt(conf.get("col_b"));
			for(int i = 0; i < numberOfColsInB; ++i) {
				String keyToBeSent = rowId + "," + i;
				System.out.println(keyToBeSent+" "+value);
				context.write(new Text(keyToBeSent), value);
			}
		} else {
			int numberOfRowsInA = Integer.parseInt(conf.get("row_a"));
			for(int i = 0; i < numberOfRowsInA; ++i) {
				String keyToBeSent = i + "," + colId;
				System.out.println(keyToBeSent+" "+value);
				context.write(new Text(keyToBeSent), value);
			}
		}
		
	}

}
