package log;

import java.util.HashMap;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MonthPartitioner<K2, V2> extends Partitioner<Text, Text> implements
		Configurable {

	private Configuration configuration;
	HashMap<String, Integer> months = new HashMap<String, Integer>();

	@Override
	public Configuration getConf() {
		// TODO Auto-generated method stub
		return configuration;
	}

	@Override
	public void setConf(Configuration configuration) {
		this.configuration = configuration;
		months.put("00", 0);
		months.put("01", 1);
		months.put("02", 2);
		months.put("03", 3);
		months.put("04", 4);
		months.put("05", 5);
		months.put("06", 6);
		months.put("07", 7);
		months.put("08", 8);
		months.put("09", 9);
		months.put("10", 10);
		months.put("11", 11);
		months.put("12", 12);
		months.put("13", 13);
		months.put("14", 14);
		months.put("15", 15);
		months.put("16", 16);
		months.put("17", 17);
		months.put("18", 18);
		months.put("19", 19);
		months.put("20", 20);
		months.put("21", 21);
		months.put("22", 22);
		months.put("23", 23);

		// TODO Auto-generated method stub

	}

	@Override
	public int getPartition(Text key, Text value, int numReduceTasks) {
		// TODO Auto-generated method stub
		return (int) months.get(value.toString());
	}

}
