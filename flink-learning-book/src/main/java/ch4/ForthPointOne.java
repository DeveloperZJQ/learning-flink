package ch4;

import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author happy
 * @since 2021-02-02
 */
public class ForthPointOne {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //读取文件源数据
        DataStreamSource<String> readTextFile = readTextFile(env);
        DataStreamSource<String> readFile = readFile(env);

        //Socket数据源
        DataStreamSource<String> socketTextStream = env.socketTextStream("127.0.0.1", 9999);

        //集合数据源
        DataStreamSource<Tuple2<Long, Long>> tuple2DataStreamSource = env.fromElements(new Tuple2<>(1L, 3L), new Tuple2<>(1L, 3L), new Tuple2<>(1L, 3L));
        DataStreamSource<String> stringDataStreamSource = env.fromCollection(Arrays.asList("hello", "world"));

        //外部数据源
    }

    /**
     * 直接读取文本文件
     */
    public static DataStreamSource<String> readTextFile(StreamExecutionEnvironment env) {
        return env.readTextFile("file:///path");
    }

    /**
     * 通过指定CSVInputFormat读取CSV文件
     */
    public static DataStreamSource<String> readFile(StreamExecutionEnvironment env) {
        // 外层父级目录
        String dir = "hdfs://path";
        Path path = new Path(dir);
        Configuration configuration = new Configuration();
        // 设置递归获取文件
        configuration.setBoolean("recursive.file.enumeration", true);

        TextInputFormat textInputFormat = new TextInputFormat(path);
        textInputFormat.supportsMultiPaths();
        textInputFormat.configure(configuration);
        textInputFormat.setFilesFilter(new FilePathFilter() {
            @Override
            public boolean filterPath(Path filePath) {
                // 过滤想要的路径
                return !filePath.toString().contains("2021-02-02");
            }
        });
        return env.readFile(textInputFormat, dir);
    }
}
