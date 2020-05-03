package com.ke.spider.digest;

import com.ke.spider.digest.configuration.*;
import com.ke.spider.digest.framework.*;
import com.ke.spider.digest.utils.AsciiArtUtils;
import com.ke.spider.digest.utils.CommandLineArgs;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.util.List;

/**
 *
 */
public class SpiderDigestSpark {

    private static final String SPARK = "spark";

    public static void main(String[] args) {
        run("D:\\Users\\zhaozhuo001\\SourceCode\\spider-data-etl\\spider-data-spark\\src\\main\\resources\\example.conf", SPARK, args);
    }

    public static void run(String configPath, String engine, String[] args) {
        try {
            entrypoint(configPath, engine);
        } catch (ConfigRuntimeException e) {
            showConfigError(e);
        } catch (Exception e) {
            showFatalError(e);
        }


    }

    private static String getConfigFilePath(CommandLineArgs cmdArgs, String engine) {
        String path = null;
        switch (engine) {
            case "flink":
                path = cmdArgs.getConfigFile();
                break;
            case "spark":
                final String mode = new Common().getDeployMode();
                if ("cluster".equals(mode)) {
                    //path = new Path(cmdArgs.getConfigFile()).getName();
                } else {
                    path = cmdArgs.getConfigFile();
                }
                break;
            default:
                break;
        }
        return path;
    }

    private static void entrypoint(String configFile, String engine) {

        ConfigBuilder configBuilder = new ConfigBuilder(configFile, engine);
        List<Source> sources = configBuilder.createPlugins("source", Source.class);
        List<Transform> transforms = configBuilder.createPlugins("transform", Transform.class);
        List<Sink> sinks = configBuilder.createPlugins("sink", Sink.class);
        Execution execution = configBuilder.createExecution();
        baseCheckConfig(sources, transforms, sinks);
        prepare(configBuilder.getEnv(), sources, transforms, sinks);
        showAsciiLogo();

        execution.start(sources, transforms, sinks);
    }

    private static void baseCheckConfig(List<? extends Plugin>... plugins) {
        boolean configValid = true;
        for (List<? extends Plugin> pluginList : plugins) {
            for (Plugin plugin : pluginList) {
                CheckResult checkResult = null;
                try {
                    checkResult = plugin.checkConfig();
                } catch (Exception e) {
                    checkResult = new CheckResult(false, e.getMessage());
                }
                if (!checkResult.isSuccess()) {
                    configValid = false;
                    System.out.println(String.format("Plugin[%s] contains invalid config, error: %s\n"
                            , plugin.getClass().getName(), checkResult.getMsg()));
                }
                if (!configValid) {
                    System.exit(-1); // invalid configuration
                }
            }
        }
    }

    private static void prepare(RuntimeEnv env, List<? extends Plugin>... plugins) {
        for (List<? extends Plugin> pluginList : plugins) {
            pluginList.forEach(plugin -> plugin.prepare(env));
        }

    }

    private static void showAsciiLogo() {
        AsciiArtUtils.printAsciiArt("SpiderDigestSpark");
    }

    private static void showConfigError(Throwable throwable) {
        System.out.println(
                "\n\n===============================================================================\n\n");
        String errorMsg = throwable.getMessage();
        System.out.println("Config Error:\n");
        System.out.println("Reason: " + errorMsg + "\n");
        System.out.println(
                "\n===============================================================================\n\n\n");
    }

    private static void showFatalError(Throwable throwable) {
        System.out.println(
                "\n\n===============================================================================\n\n");
        String errorMsg = throwable.getMessage();
        System.out.println("Fatal Error, \n");
        System.out.println(
                "Please contact garygaowork@gmail.com or issue a bug in https://github.com/InterestingLab/waterdrop/issues\n");
        System.out.println("Reason: " + errorMsg + "\n");
        System.out.println("Exception StackTrace: " + ExceptionUtils.getStackTrace(throwable));
        System.out.println(
                "\n===============================================================================\n\n\n");
    }
}
