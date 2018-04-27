import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

/**
 * Created by zhujiadong on 2018/4/27 16:48
 */

public class Hdfs {
    public static void main(String[] args) throws IOException {
        FileSystem fileSystem = FileSystem.get(URI.create("hdfs://zhujiadong:9000"), new Configuration());
        fileSystem.copyToLocalFile(new Path("/hadoop-2.7.4.tar.gz"), new Path("D:\\hadoop.tar.gz"));

    }
}
