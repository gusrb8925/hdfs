package extcount2;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ExtCount {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			System.out.printf("Usage: ExtCount2 <input dir> <output div>\n");
			System.exit(-1);
		}

		Job job = new Job();

		job.setJarByClass(ExtCount.class);

		job.setJobName("Ext Count2");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(ExtMapper.class);
		job.setReducerClass(SumReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);

	}

}
