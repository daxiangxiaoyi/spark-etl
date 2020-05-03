package com.ke.spider.digest.configuration;


import com.ke.spider.digest.framework.*;
import com.ke.spider.digest.framework.spark.SparkEnvironment;
import com.ke.spider.digest.framework.spark.batch.SparkBatchExecution;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigResolveOptions;

import java.io.File;
import java.util.*;

/**
 * @author mr_xiong
 * @date 2019-12-29 14:56
 * @description
 */
public class ConfigBuilder {

    private static final String PLUGIN_NAME_KEY = "plugin_name";
    private String configFile;
    private String engine;
    private ConfigPackage configPackage;
    private Config config;
    private boolean streaming;
    private Config envConfig;
    private RuntimeEnv env;

    public ConfigBuilder(String configFile, String engine) {
        this.configFile = configFile;
        this.engine = engine;
        this.configPackage = new ConfigPackage(engine);
        this.config = load();
        this.env = createEnv();
    }

    public ConfigBuilder(String configFile) {
        this.configFile = configFile;
        this.engine = "";
        this.config = load();
        this.env = createEnv();
    }

    private Config load() {

        if (configFile.isEmpty()) {
            throw new ConfigRuntimeException("Please specify config file");
        }

        System.out.println("[INFO] Loading config file: " + configFile);

        // variables substitution / variables resolution order:
        // config file --> system environment --> java properties
        Config config = ConfigFactory
                .parseFile(new File(configFile))
                .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
                .resolveWith(ConfigFactory.systemProperties(),
                        ConfigResolveOptions.defaults().setAllowUnresolved(true));

        ConfigRenderOptions options = ConfigRenderOptions.concise().setFormatted(true);
        System.out.println("[INFO] parsed config file: " + config.root().render(options));
        return config;
    }


    public Config getEnvConfigs() {
        return envConfig;
    }

    public RuntimeEnv getEnv() {
        return env;
    }

    private boolean checkIsStreaming() {
        List<? extends Config> sourceConfigList = config.getConfigList("source");

        return sourceConfigList.get(0).toString().toLowerCase().endsWith("stream");
    }

    /**
     * Get full qualified class name by reflection api, ignore case.
     **/
    private String buildClassFullQualifier(String name, String classType)  {
        Map<String,String> map=new HashMap<>();
        Plugin plugin = null;
        if (name.split("\\.").length == 1) {
            String packageName = null;

            switch (classType) {
                case "source":
                    packageName = configPackage.sourcePackage+"."+name;
                   name=configPackage.baseSourcePackage;
                  // plugin=(SparkSource)Class.forName(packageName).newInstance();
                    break;
                case "transform":
                    packageName = configPackage.transformPackage+"."+name;
                    name=configPackage.baseTransformPackage;
                    //plugin=(SparkTransform)Class.forName(packageName).newInstance();
                    break;
                case "sink":
                    packageName = configPackage.sinkPackage+"."+name;
                    name=configPackage.baseSinkPackage;
                   // plugin=(SparkSink)Class.forName(packageName).newInstance();
                    break;
                default:
                    break;
            }
            map.put("parent",packageName);
            map.put("son",name);
            name=packageName;
        }
            return name;

    }


    /**
     * check if config is valid.
     **/
    public void checkConfig() {
        this.createEnv();
        this.createPlugins("source", Source.class);
        this.createPlugins("transform", Transform.class);
        this.createPlugins("sink", Sink.class);
    }

    public <T extends Plugin> List<T> createPlugins(String type, Class<T> clazz) {

        List<T> basePluginList = new ArrayList<>();

        if(!config.hasPath(type)){
            return basePluginList;
        }

        List<? extends Config> configList = config.getConfigList(type);

        configList.forEach(plugin -> {
            try {
                String root=plugin.root().unwrapped().keySet().toArray()[0].toString();
                String className=root+type.substring(0,1).toUpperCase() + type.substring(1);
                final String n = buildClassFullQualifier(className, type);
                T t = (T) Class.forName(n).newInstance();
                t.setConfig(plugin.getConfig(root));
                basePluginList.add(t);

            } catch (Exception e) {
                e.printStackTrace();
            }
      });

        return basePluginList;
    }

    private RuntimeEnv createEnv() {
        envConfig = config.getConfig("env");
        streaming = checkIsStreaming();
        RuntimeEnv env = null;
        switch (engine) {
            case "spark":
                env = new SparkEnvironment();
                break;
            case "flink":
                env = null;
                break;
            default:
                break;
        }
        env.setConfig(envConfig);
        env.prepare(streaming);
        return env;
    }


    public Execution createExecution() {
        Execution execution = null;
        switch (engine) {
            case "spark":
                SparkEnvironment sparkEnvironment = (SparkEnvironment) env;
                if (streaming) {
                    execution = new SparkBatchExecution(sparkEnvironment);
                } else {
                    execution = new SparkBatchExecution(sparkEnvironment);
                }
                break;
            default:
                break;
        }
        return execution;
    }


}
