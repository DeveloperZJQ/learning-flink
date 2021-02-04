package ch4;

import org.apache.flink.api.common.functions.Partitioner;

import java.util.Random;

/**
 * @author happy
 * @since 2021-02-03
 * 用户自定义分区
 */
public class UserDefinePartitioner implements Partitioner<String> {

    @Override
    public int partition(String s, int i) {
        Random random = new Random();
        if (s.toLowerCase().contains("flink")){
            return 0;
        }else {
            return random.nextInt(i);
        }
    }
}
