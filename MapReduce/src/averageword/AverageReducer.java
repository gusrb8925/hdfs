package averageword;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageReducer extends
		Reducer<Text, IntWritable, Text, DoubleWritable> {

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		long num = 0, wordCount = 0;

		for (IntWritable value : values) {
			wordCount += value.get();
			num++;
		}

		if (num != 0) {
			double res = (double) wordCount / (double) num;
			context.write(key, new DoubleWritable(res));
		}
	}
}
