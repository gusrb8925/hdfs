package log;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ProceessLogs extends Configured implements Tool {

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		int exitCode = ToolRunner.run(conf, new ProceessLogs(), args);
		System.exit(exitCode);
	}

	/**
	 * @param args
	 */
	public int run(String[] args) throws Exception {

		if (args.length != 2) {
			System.out.printf("Usage: WordCount <input dir> <output div>\n");
			System.exit(-1);
		}

		Job job = new Job();
		job.setJarByClass(ProceessLogs.class);
		job.setJobName("ProceessLogs");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(LogMonthMapper.class);
		job.setReducerClass(CountReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setNumReduceTasks(24);

		job.setPartitionerClass(MonthPartitioner.class);

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;

	}

}
