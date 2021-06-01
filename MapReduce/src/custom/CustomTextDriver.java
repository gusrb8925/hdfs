package custom;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CustomTextDriver extends Configured implements Tool {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(),
				new CustomTextDriver(), args);
		System.exit(exitCode);

	}

	@Override
	public int run(String[] args) throws Exception {

		if (args.length != 2) {
			System.out
					.printf("Usage: CustomTextDriver <input dir> <output div>\n");
			return -1;
		}

		Job job = new Job(getConf());
		job.setJarByClass(CustomTextDriver.class);
		job.setJobName("CustomTextDriver");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(CustomTextMapper.class);
		job.setReducerClass(LongSumReducer.class);

		job.setOutputKeyClass(CustomTextWritable.class);
		job.setOutputValueClass(LongWritable.class);

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

}
