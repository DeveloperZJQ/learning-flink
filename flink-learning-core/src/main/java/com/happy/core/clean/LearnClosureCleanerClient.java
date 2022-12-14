package com.happy.core.clean;

import com.happy.core.clean.entity.Human;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.ClosureCleaner;

/**
 * @author DeveloperZJQ
 * @link org.apache.flink.api.java.ClosureCleaner
 * @since 2022-12-14
 */
public class LearnClosureCleanerClient {
    public static void main(String[] args) {
        Human human = new Human();
//        Human.IdentityInfo identityInfo = human.new IdentityInfo();
        ClosureCleaner.clean(human, ExecutionConfig.ClosureCleanerLevel.TOP_LEVEL, false);
        ClosureCleaner.ensureSerializable(human);

    }
}
