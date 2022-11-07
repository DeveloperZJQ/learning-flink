package org.apache.flink.client.cli;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;


/**
 * @author happy
 * @since 2022/7/24
 */
public class CliFrontend {
    private static final Logger LOG = LoggerFactory.getLogger(CliFrontend.class);

    private static final String CONFIG_DIRECTORY_FALLBACK_1 = "../conf";
    private static final String CONFIG_DIRECTORY_FALLBACK_2 = "conf";

    public static String getConfigurationDirectoryFromEnv() {
        String location = System.getenv(ConfigConstants.ENV_FLINK_CONF_DIR);

        if (location != null) {
            if (new File(location).exists()) {
                return location;
            } else {
                throw new RuntimeException(
                        "The configuration directory '"
                                + location
                                + "', specified in the '"
                                + ConfigConstants.ENV_FLINK_CONF_DIR
                                + "' environment variable, does not exist.");
            }
        } else if (new File(CONFIG_DIRECTORY_FALLBACK_1).exists()) {
            location = CONFIG_DIRECTORY_FALLBACK_1;
        } else if (new File(CONFIG_DIRECTORY_FALLBACK_2).exists()) {
            location = CONFIG_DIRECTORY_FALLBACK_2;
        } else {
            throw new RuntimeException(
                    "The configuration directory was not specified. "
                            + "Please specify the directory containing the configuration file through the '"
                            + ConfigConstants.ENV_FLINK_CONF_DIR
                            + "' environment variable.");
        }
        return location;
    }

    public static List<CustomCommandLine> loadCustomCommandLines(
            Configuration configuration, String configurationDirectory) {
        // 声明命令行集合
        List<CustomCommandLine> customCommandLines = new ArrayList<>();
        customCommandLines.add(new GenericCLI(configuration, configurationDirectory));

        //	Command line interface of the YARN session, with a special initialization here
        //	to prefix all options with y/yarn.
        final String flinkYarnSessionCLI = "org.apache.flink.yarn.cli.FlinkYarnSessionCli";
        try {
            customCommandLines.add(
                    loadCustomCommandLine(
                            flinkYarnSessionCLI,
                            configuration,
                            configurationDirectory,
                            "y",
                            "yarn"));
        } catch (NoClassDefFoundError | Exception e) {
            final String errorYarnSessionCLI = "org.apache.flink.yarn.cli.FallbackYarnSessionCli";
            try {
                LOG.info("Loading FallbackYarnSessionCli");
                customCommandLines.add(loadCustomCommandLine(errorYarnSessionCLI, configuration));
            } catch (Exception exception) {
                LOG.warn("Could not load CLI class {}.", flinkYarnSessionCLI, e);
            }
        }

        //	Tips: DefaultCLI must be added at last, because getActiveCustomCommandLine(..) will get
        // the
        //	      active CustomCommandLine in order and DefaultCLI isActive always return true.
        customCommandLines.add(new DefaultCLI());

        return customCommandLines;
    }

    /**
     * Loads a class from the classpath that implements the CustomCommandLine interface.
     *
     * @param className The fully-qualified class name to load.
     * @param params    The constructor parameters
     */
    private static CustomCommandLine loadCustomCommandLine(String className, Object... params)
            throws Exception {

        Class<? extends CustomCommandLine> customCliClass =
                Class.forName(className).asSubclass(CustomCommandLine.class);

        // construct class types from the parameters
        Class<?>[] types = new Class<?>[params.length];
        for (int i = 0; i < params.length; i++) {
            checkNotNull(params[i], "Parameters for custom command-lines may not be null.");
            types[i] = params[i].getClass();
        }

        Constructor<? extends CustomCommandLine> constructor = customCliClass.getConstructor(types);

        return constructor.newInstance(params);
    }

    public static void main(String[] args) {
        // 获取 JVM 信息、hadoop 信息等打印日志
        EnvironmentInformation.logEnvironmentInfo(LOG, "Command Line Client", args);

        // 1. find the configuration directory (找配置目录，即conf/flink-conf.yaml)
        final String configurationDirectory = getConfigurationDirectoryFromEnv();

        // 2. load the global configuration (加载全局配置)
        final Configuration configuration = GlobalConfiguration.loadConfiguration(configurationDirectory);

        // 3. load the custom command lines (加载客户命令行)
        // 初始化 3 种不同的 CLI 分别是 GenericCLI 对应的是 per-job 模式，flinkYarnSessionCLI 对应的是 yarn-session 模式，以及 DefaultCLI 对应的是 standalone 模式
        final List<CustomCommandLine> customCommandLines = loadCustomCommandLines(configuration, configurationDirectory);

    }
}
