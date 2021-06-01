package custom;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CustomTextMapper extends
		Mapper<LongWritable, Text, CustomTextWritable, LongWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		LongWritable one = new LongWritable(1);

		String[] fields = value.toString().split(" ");

		if (fields.length > 3) {

			String ip = fields[0];

			String[] dtFields = fields[3].split("/");
			if (dtFields.length > 1) {
				String theMonth = dtFields[1];

				context.write(new CustomTextWritable(theMonth, ip), one);
			}
		}

	}

}
