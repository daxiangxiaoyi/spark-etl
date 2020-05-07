package com.daxiang.digest.configuration;

import com.daxiang.digest.framework.Sink;
import com.daxiang.digest.framework.Source;
import com.daxiang.digest.framework.Plugin;
import com.daxiang.digest.framework.Transform;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigResolveOptions;
import org.apache.commons.configuration.ConfigurationRuntimeException;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * @author zhaozhuo
 * @date 2020/4/29
 */
public class ConfigBuilderv1<T> {
    private static final String PackagePrefix = "com.ke.spider.data";
    private static final String FilterPackage = PackagePrefix + ".filter";
    private static final String SourcePackage = PackagePrefix + ".source";
    private static final String SinkPackage = PackagePrefix + ".sink";

    private static String PluginNameKey = "plugin_name";
    private String configFile;

    public ConfigBuilderv1(String configFile) {
        this.configFile = configFile;
    }

    private Config config = load();

    Config load() {

        // val configFile = System.getProperty("config.path", "")
        if (configFile == "") {
            throw new ConfigurationRuntimeException("Please specify config file");
        }

        System.out.println(("[INFO] Loading config file: " + configFile));

        // variables substitution / variables resolution order:
        // config file --> syste environment --> java properties
        Config config = ConfigFactory.parseFile(new File(configFile)).resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
                .resolveWith(ConfigFactory.systemProperties(), ConfigResolveOptions.defaults().setAllowUnresolved(true));
        ConfigRenderOptions options = ConfigRenderOptions.concise().setFormatted(true);
        return config;

    }


    void checkConfig() {
        Config sparkConfig = this.getSparkConfigs();
        List<Source> sources = this.createStaticInputs("batch");
        List<Sink> sinks = this.createOutputs("batch");
        List<Transform> transforms = this.createFilters();
    }

    public Config getSparkConfigs() {
        return config.getConfig("spark");
    }

   public List<Transform> createFilters() {
        List<Transform> transformList = new ArrayList<>();
        config.getConfigList("filter").forEach(component -> {
            String className = buildClassFullQualifier(component.getString(ConfigBuilderv1.PackagePrefix), "filter");
            try {
                Transform obj = (Transform) Class.forName(className).newInstance();
                obj.setConfig(component);
                transformList.add(obj);
            } catch (InstantiationException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        });
        return transformList;
    }


   public List<Source> createStaticInputs(String engine) {
        List<Source> sourceList = new ArrayList<>();
        config.getConfigList("source").forEach(compent -> {
            String className = buildClassFullQualifier(compent.getString(ConfigBuilderv1.PluginNameKey), "input", engine);
            try {
                Object obj = Class.forName(className).newInstance();
                if (obj instanceof Source) {
                    Source source = (Source) obj;
                    sourceList.add(source);
                }
            } catch (InstantiationException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        });

        return sourceList;
    }

    public List<Sink> createOutputs(String engine) {

        List<Sink> outputList = new ArrayList<>();
        config.getConfigList("sink").forEach(component -> {
            String className = null;
            if ("batch".equals(engine) || "sparkstreaming".equals(engine)) {
                className = buildClassFullQualifier(component.getString(ConfigBuilderv1.PluginNameKey), "output", "batch");
            }

            if ("structuredstreaming".equals(engine)) {
                className = buildClassFullQualifier(component.getString(ConfigBuilderv1.PluginNameKey), "output", engine);
            }
            try {
                Sink obj = (Sink) Class.forName(className).newInstance();
                obj.setConfig(component);
                outputList.add(obj);

            } catch (InstantiationException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        });

        return outputList;
    }

    private String getInputType(String name, String engine) {
        String type = null;
        if (name.toLowerCase().endsWith("stream")) {
            if ("batch".equals(engine)) {
                type = "sparkstreaming";
            }
            if ("structuredstreaming".equals(engine)) {
                type = "structuredstreaming";
            }
        } else {
            type = "batch";
        }
        return type;
    }

    /**
     * Get full qualified class name by reflection api, ignore case.
     */
    private String buildClassFullQualifier(String name, String classType) {
        return buildClassFullQualifier(name, classType, "");
    }

    private String buildClassFullQualifier(String name, String classType, String engine) {

        String qualifier = name;
        if (qualifier.split("\\.").length == 1) {
            String packageName = null;
            switch (classType) {
                case "source":
                    packageName = ConfigBuilderv1.SourcePackage + "." + getInputType(name, engine);
                    break;
                case "sink":
                    packageName = ConfigBuilderv1.SinkPackage + "." + engine;
                    break;
                case "filter":
                    packageName = ConfigBuilderv1.FilterPackage;
                    break;
            }


            List<Plugin> services = new ArrayList<>();


            Boolean classFound = false;
            breakable:
            {
                for (Plugin serviceInstance : services) {
                    Class clz = serviceInstance.getClass();
                    // get class name prefixed by package name
                    String clzNameLowercase = clz.getName().toLowerCase();
                    String qualifierWithPackage = packageName + "." + qualifier;
                    if (clzNameLowercase == qualifierWithPackage.toLowerCase()) {
                        qualifier = clz.getName();
                        classFound = true;
                        break;
                    }
                }
            }
        }

        return qualifier;
    }
}
