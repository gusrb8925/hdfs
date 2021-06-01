package seq;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CreateUncomoressedSequenceFile extends Configured implements Tool {

	public static void main(String[] args) throws Exception {

		int exitCode = ToolRunner.run(new Configuration(),
				new CreateUncomoressedSequenceFile(), args);

		System.exit(exitCode);
	}

	/**
	 * @param args
	 */
	public int run(String[] args) throws Exception {

		if (args.length != 2) {
			System.out
					.printf("Usage: CreateUncomoressedSequenceFile <input dir> <output div>\n");
			System.exit(-1);
		}

		Job job = new Job(getConf());
		job.setJarByClass(CreateUncomoressedSequenceFile.class);
		job.setJobName("CreateUncomoressedSequenceFile");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setNumReduceTasks(0);

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;

	}

}
