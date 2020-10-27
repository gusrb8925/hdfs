package hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsWrite {
	public static void main(String[] args) {

		try {

			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://192.168.26.131:9000");
			FileSystem hdfs = FileSystem.get(conf);

			System.out.println(hdfs.getHomeDirectory());
			System.out.println(hdfs.getWorkingDirectory());

			Path path = new Path("/localFile1");
			Path localPath = new Path("C:\\Users\\DATA8320-06\\Downloads\\twitter.csv");

			System.out.println(hdfs.exists(path));
			if (hdfs.exists(path)) {
				hdfs.delete(path, true);
			}

			// 파일 업로드 끝!
			hdfs.copyFromLocalFile(localPath, path);

			System.out.println("Local FIle Upload Finished!");

			hdfs.close();

		} catch (Exception e) {
			System.out.println(e.toString());
		}

	}

}
