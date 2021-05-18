package tool;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	boolean caseSensitive = false;

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();

		for (String word : line.split("\\W+")) {
			if (word.length() > 0) {

				String letter = "";

				if (caseSensitive) {
					letter = word.substring(0, 1).toUpperCase();
				} else {
					letter = word.substring(0, 1).toLowerCase();
				}
				context.write(new Text(letter), new IntWritable(1));
			}
		}

	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		caseSensitive = conf.getBoolean("caseSensitive", false);
	}

}
