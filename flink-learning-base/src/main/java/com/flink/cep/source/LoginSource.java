package com.flink.cep.source;

import com.flink.cep.entity.LoginEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @author : DeveloperZJQ
 * @since 2022-06-18
 */
public class LoginSource extends RichSourceFunction<LoginEvent> {

    List<String> loginStatusList;
    List<Integer> userIdList;
    List<String> userNameList;
    Random random;
    Boolean isRunning;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        loginStatusList = Arrays.asList("failed", "failed");
        userIdList = Arrays.asList(1, 2, 3, 4, 5);
        userNameList = Arrays.asList("zhangsan", "lisi", "wangwu", "maliu", "yanqi");
        random = new Random();
        isRunning = true;
    }

    @Override
    public void run(SourceContext<LoginEvent> ctx) throws Exception {
        while (isRunning) {
            Long currentTimeStamp = System.currentTimeMillis();
            final int index = random.nextInt(5);
            int id = userIdList.get(index);
            String userName = userNameList.get(index);
            final int statusIndex = random.nextInt(2);
            final String status = loginStatusList.get(statusIndex);
            ctx.collect(new LoginEvent(id, userName, status, currentTimeStamp));
            TimeUnit.SECONDS.sleep(1);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
        loginStatusList = new ArrayList<>();
        userIdList = new ArrayList<>();
        userNameList = new ArrayList<>();
    }
}