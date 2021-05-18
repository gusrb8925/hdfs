package iptool;

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

public class IpAreaCount extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("ipType", "2");
		int exitCode = ToolRunner.run(conf, new IpAreaCount(), args);
		System.exit(exitCode);
	}

	/**
	 * @param args
	 */
	public int run(String[] args) throws Exception {

		if (args.length != 2) {
			System.out.printf("Usage: ExtCount2 <input dir> <output div>\n");
			System.exit(-1);
		}

		Job job = new Job(getConf());
		job.setJarByClass(IpAreaCount.class);
		job.setJobName("Ip Count");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(IpAreaMapper.class);
		job.setReducerClass(SumReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;

	}

}
