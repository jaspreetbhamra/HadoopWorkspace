package flajolet_martin;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperFM extends Mapper<LongWritable, Text, Text, LongWritable> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		long input = Long.parseLong(value.toString());
//		Hash function: (3x+1) mod 5
		long hashed = (77*input + 1) % 1000000;
		
		boolean flag = true;
//		System.out.println(hashed);
		String binary = Long.toBinaryString(hashed);
		long count = 0;
//		System.out.println(binary);
		for(int i = binary.length()-1; i>=0; --i) {
			if(binary.charAt(i)=='0') 
				count++;
			if(binary.charAt(i)=='1') {
				flag = false;
				break;
			}
		}
		if(count==binary.length())
			count = 0;
//		System.out.println(count);
		context.write(new Text("Count"), new LongWritable(count));
	}

}
