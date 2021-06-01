package maponly;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ImageCounterMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] fields = value.toString().split("\"");
		if (fields.length > 1) {
			String request = fields[1];
			fields = request.split(" ");

			if (fields.length > 1) {
				String fileName = fields[1].toLowerCase();

				if (fileName.endsWith(".jpg"))
					context.getCounter("ImageCounter", "jpg").increment(1);
				else if (fileName.endsWith(".gif"))
					context.getCounter("ImageCounter", "gif").increment(1);
				else
					context.getCounter("ImageCounter", "other").increment(1);
			}
		}

	}

}
