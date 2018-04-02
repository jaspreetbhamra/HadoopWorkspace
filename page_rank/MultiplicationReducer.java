package page_rank;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MultiplicationReducer extends Reducer<Text, Text, Text, Text> {

	DecimalFormat df = new DecimalFormat("#.####");
	
	public void reduce(Text _key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// process values
//		for (Text val : values) {
//
//		}
		Configuration conf = context.getConfiguration();
		int row_dimension = Integer.parseInt(conf.get("col_a"));
		int col_dimension = Integer.parseInt(conf.get("row_b"));
		double[] row = new double[row_dimension];
		double col[] = new double[col_dimension];
		for(Text val: values) {
			String data[] = val.toString().split(",");
			if(data[0].trim().equals("a")) {
				int colIndex = Integer.parseInt(data[2].trim());
				double numberData = Double.parseDouble(data[3].trim());
				row[colIndex] = numberData;
//				System.out.println(colIndex+" "+numberData);
			} else {
				int rowIndex = Integer.parseInt(data[1].trim());
				double numberData = Double.parseDouble(data[3].trim());
				col[rowIndex] = numberData;
//				System.out.println(rowIndex+" "+numberData);
			}
		}
		double product = 0;
		for(int i = 0; i < row_dimension; ++i) {
			product += (row[i]*col[i]);
//			System.out.println("****"+product+"****");
		}
		System.out.println("****"+product+"****");
		String sendThis = df.format(product);	//Double number with a precision value of 2
		context.write(new Text("b,"+_key+","+sendThis), new Text(""));
//		context.write(new Text(), new Text());
//		context.write(new Text(sendThis+"\n"), new Text());
	}

}
