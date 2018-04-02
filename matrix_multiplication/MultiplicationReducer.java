package matrix_multiplication;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MultiplicationReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text _key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// process values
//		for (Text val : values) {
//
//		}
		Configuration conf = context.getConfiguration();
		int row_dimension = Integer.parseInt(conf.get("col_a"));
		int col_dimension = Integer.parseInt(conf.get("row_b"));
		int[] row = new int[row_dimension];
		int col[] = new int[col_dimension];
		for(Text val: values) {
			String data[] = val.toString().split(",");
			if(data[0].equals("a")) {
				int colIndex = Integer.parseInt(data[2]);
				int numberData = Integer.parseInt(data[3]);
				row[colIndex] = numberData;
				System.out.println(colIndex+" "+numberData);
			} else {
				int rowIndex = Integer.parseInt(data[1]);
				int numberData = Integer.parseInt(data[3]);
				col[rowIndex] = numberData;
				System.out.println(rowIndex+" "+numberData);
			}
		}
		int product = 0;
		for(int i = 0; i < row_dimension; ++i) {
			product += (row[i]*col[i]);
			System.out.println("****"+product+"****");
		}
//		context.write(_key, new IntWritable(product));
	}

}
