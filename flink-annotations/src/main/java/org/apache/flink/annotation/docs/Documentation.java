package org.apache.flink.annotation.docs;

import org.apache.flink.annotation.Internal;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 用于修改文档生成器行为的注释集合。
 *
 * @author DeveloperZJQ
 * @since 2022-7-19
 */
public final class Documentation {

    /**
     * 使用此注解代表覆盖文档默认值
     **/
    @Target(ElementType.FIELD)
    @Retention(RetentionPolicy.RUNTIME)
    @Internal
    public @interface OverrideDefault {
        String value();
    }

    /**
     * 用于配置选项字段的注释，以将它们包含在特定部分中。部分跨选项类聚合的选项组，放置每个组放到一个专用文件中。
     * 参数{@link Section#position()}控制生成表中的位置，使用较低的值被放置在顶部。相同位置的字段按字母顺序排序的关键。
     */
    @Target(ElementType.FIELD)
    @Retention(RetentionPolicy.RUNTIME)
    @Internal
    public @interface Section {
        /**
         * 配置文档中应该包含此选项的部分。
         */
        String[] value() default {};

        /**
         * 该选项在其部分中的相对位置。
         */
        int position() default Integer.MAX_VALUE;
    }

    /**
     * 选项静态名
     */
    public static final class Sections {

        public static final String COMMON_HOST_PORT = "common_host_port";
        public static final String COMMON_STATE_BACKENDS = "common_state_backends";
        public static final String COMMON_HIGH_AVAILABILITY = "common_high_availability";
        public static final String COMMON_HIGH_AVAILABILITY_ZOOKEEPER =
                "common_high_availability_zk";
        public static final String COMMON_HIGH_AVAILABILITY_JOB_RESULT_STORE =
                "common_high_availability_jrs";
        public static final String COMMON_MEMORY = "common_memory";
        public static final String COMMON_MISCELLANEOUS = "common_miscellaneous";

        public static final String SECURITY_SSL = "security_ssl";
        public static final String SECURITY_AUTH_KERBEROS = "security_auth_kerberos";
        public static final String SECURITY_AUTH_ZOOKEEPER = "security_auth_zk";

        public static final String STATE_BACKEND_ROCKSDB = "state_backend_rocksdb";

        public static final String STATE_BACKEND_LATENCY_TRACKING =
                "state_backend_latency_tracking";

        public static final String STATE_BACKEND_CHANGELOG = "state_backend_changelog";

        public static final String EXPERT_CLASS_LOADING = "expert_class_loading";
        public static final String EXPERT_DEBUGGING_AND_TUNING = "expert_debugging_and_tuning";
        public static final String EXPERT_SCHEDULING = "expert_scheduling";
        public static final String EXPERT_FAULT_TOLERANCE = "expert_fault_tolerance";
        public static final String EXPERT_STATE_BACKENDS = "expert_state_backends";
        public static final String EXPERT_REST = "expert_rest";
        public static final String EXPERT_HIGH_AVAILABILITY = "expert_high_availability";
        public static final String EXPERT_ZOOKEEPER_HIGH_AVAILABILITY =
                "expert_high_availability_zk";
        public static final String EXPERT_KUBERNETES_HIGH_AVAILABILITY =
                "expert_high_availability_k8s";
        public static final String EXPERT_SECURITY_SSL = "expert_security_ssl";
        public static final String EXPERT_ROCKSDB = "expert_rocksdb";
        public static final String EXPERT_CLUSTER = "expert_cluster";
        public static final String EXPERT_JOB_MANAGER = "expert_jobmanager";

        public static final String ALL_JOB_MANAGER = "all_jobmanager";
        public static final String ALL_TASK_MANAGER = "all_taskmanager";
        public static final String ALL_TASK_MANAGER_NETWORK = "all_taskmanager_network";

        public static final String DEPRECATED_FILE_SINKS = "deprecated_file_sinks";

        public static final String METRIC_REPORTERS = "metric_reporters";

        private Sections() {
        }
    }

    /**
     * 表配置选项中用于添加元数据标签的注释。
     * <p>
     * {@link TableOption#execMode()}参数表示配置工作的执行模式(批处理、流处理或两者兼有)。
     */
    @Target(ElementType.FIELD)
    @Retention(RetentionPolicy.RUNTIME)
    @Internal
    public @interface TableOption {
        ExecMode execMode();
    }

    /**
     * 执行模式根据配置的工作
     */
    public enum ExecMode {
        BATCH("Batch"),
        STREAMING("Streaming"),
        BATCH_STREAMING("Batch and Streaming");

        private final String name;

        ExecMode(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    /**
     * 用于配置选项字段或选项类的注释，将它们标记为后缀选项;
     * 例如，一个配置选项，其中键仅为后缀，前缀为动态在运行时提供。
     */
    @Target({ElementType.FIELD, ElementType.TYPE})
    @Retention(RetentionPolicy.RUNTIME)
    @Internal
    public @interface SuffixOption {
        String value();
    }

    /**
     * 配置选项字段或REST API消息头上用于排除它的注释文档
     */
    @Target({ElementType.FIELD, ElementType.TYPE})
    @Retention(RetentionPolicy.RUNTIME)
    @Internal
    public @interface ExcludeFromDocumentation {
        /**
         * 从文档中排除它的可选原因。
         */
        String value() default "";
    }

    private Documentation() {
    }
}