package iptool;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IpAreaMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	String ipType = "2";

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		String ipArea = "";
		int forcnt = 0;

		for (String ip : line.split("\\W+")) {
			if (ip.length() > 0) {
				forcnt++;

				if (forcnt == Integer.parseInt(ipType)) {

					int ipNum = Integer.parseInt(ip);

					if (ipNum > 0 && ipNum < 128) {
						ipArea = "A_class";
					} else if (ipNum > 128 && ipNum < 197) {
						ipArea = "B_class";
					} else if (ipNum > 197 && ipNum < 256) {
						ipArea = "C_class";
					}

					context.write(new Text(ipArea), new IntWritable(1));
					break;
				}
			}
		}
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		ipType = conf.get("ipType");
	}

}
