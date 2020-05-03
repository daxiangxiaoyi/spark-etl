package com.ke.spider.digest;

import com.ke.spider.digest.configuration.ConfigBuilderv1;
import com.ke.spider.digest.framework.Plugin;
import com.ke.spider.digest.configuration.CheckResult;
import com.ke.spider.digest.configuration.ConfigRuntimeException;
import com.ke.spider.digest.filter.UdfRegister;
import com.ke.spider.digest.framework.Transform;
import com.ke.spider.digest.framework.Sink;
import com.ke.spider.digest.framework.Source;
import com.typesafe.config.Config;
import org.apache.commons.configuration.ConfigurationRuntimeException;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author zhaozhuo
 * @date 2020/4/29
 */
public class SpiderDigest<T> {
//    Map<String, String> viewTableMap = new HashMap<>();
//
//    public static void main(String[] args) {
//
//    }
//
//
//
//    private void entrypoint(String configFile) {
//
//        ConfigBuilderv1 configBuilder = new ConfigBuilderv1(configFile);
//        SparkConf sparkConf = createSparkConf(configBuilder);
//        Tuple2<String, String>[] tuple2 = sparkConf.getAll();
//
//
//        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
//
//        // find all user defined UDFs and register in application init
//        UdfRegister.findAndRegisterUdfs(sparkSession);
//
//        List<Source> staticInputs = configBuilder.createStaticInputs("batch");
//
//        List<Transform> transforms = configBuilder.createFilters();
//        List<Sink> outputs = configBuilder.createOutputs("batch");
//
//
//        batchProcessing(sparkSession, configBuilder, staticInputs, transforms, outputs);
//
//    }
//
//
//
//
//    private void batchProcessing(
//            SparkSession sparkSession
//            , ConfigBuilderv1 configBuilder
//            , List<Source> sourceList
//            , List<Transform> transformList
//            , List<Sink> sinkList) {
//
//
//        basePrepare(sparkSession, sinkList, transformList, sinkList);
//
//        if (!sourceList.isEmpty()) {
//            Dataset<Row> ds = sourceList.get(0).getDataset(sparkSession);
//
//            for (Transform f : transformList) {
//                ds = filterProcess(sparkSession, f, ds);
//                registerFilterTempView(f, ds);
//            }
//
//            for (Sink s : sinkList) {
//                outputProcess(sparkSession, s, ds);
//            }
//
//        } else {
//            throw new ConfigRuntimeException("Input must be configured at least once.");
//        }
//
//    }
//
//    private void basePrepare(SparkSession sparkSession, List<? extends Plugin>... components) {
//        for (List<? extends Plugin> componentList : components) {
//            componentList.forEach(c -> {
//                c.prepare(sparkSession);
//            });
//        }
//    }
//
//    private Dataset<Row> filterProcess(
//            SparkSession sparkSession,
//            Transform transform,
//            Dataset<Row> ds) {
//        Config config = transform.getConfig();
//        if (config.hasPath("source_table_name")) {
//            String sourceTableName = config.getString("source_table_name");
//            sparkSession.read().table(sourceTableName);
//        }
//        return transform.process(sparkSession, ds);
//    }
//
//    private void outputProcess(SparkSession sparkSession, Sink sink, Dataset<Row> ds) {
//        Config config = sink.getConfig();
//        Dataset<Row> fromDs = null;
//        if (config.hasPath("source_table_name")) {
//            String sourceTableName = config.getString("source_table_name");
//            sparkSession.read().table(sourceTableName);
//        } else {
//            fromDs = ds;
//        }
//
//
//        sink.process(fromDs);
//    }
//
//    private void registerSourceTempView(
//            List<Source> sourceList,
//            SparkSession spark) {
//        sourceList.forEach(source -> {
//            Dataset<Row> ds = source.getDataset(spark);
//            registerSourceTempView(source, ds);
//        });
//
//    }
//
//    private void registerSourceTempView(Source source, Dataset<Row> ds) {
//        Config config = source.getConfig();
//        String tableName;
//        if (config.hasPath("table_name") || config.hasPath("result_table_name")) {
//
//            if (config.hasPath("table_name")) {
//                tableName = config.getString("table_name");
//            } else {
//                tableName = config.getString("result_table_name");
//            }
//            registerTempView(tableName, ds);
//
//        } else {
//            throw new ConfigurationRuntimeException(
//                    "Plugin[" + source.getClass().getName()+ "] must be registered as dataset/table, please set \"result_table_name\" config"
//            );
//        }
//    }
//
//    private void registerFilterTempView(Plugin plugin, Dataset<Row> ds) {
//        Config config = plugin.getConfig();
//
//        if (config.hasPath("result_table_name")) {
//            String tableName = config.getString("result_table_name");
//            registerTempView(tableName, ds);
//        }
//    }
//
//    private void registerTempView(String tableName, Dataset<Row> ds) {
//        if (viewTableMap.containsKey(tableName)) {
//
//            throw new ConfigurationRuntimeException("Detected duplicated Dataset["
//                    + tableName + "], it seems that you configured result_table_name = \"" + tableName + "\" in multiple static inputs");
//        } else {
//            ds.createOrReplaceTempView(tableName);
//            viewTableMap.put(tableName, "");
//        }
//
//    }
//
//    private SparkConf createSparkConf(ConfigBuilderv1 configBuilder) {
//        SparkConf sparkConf = new SparkConf();
//        configBuilder.getSparkConfigs().entrySet().forEach(entry -> {
//            sparkConf.set(entry.getKey(), String.valueOf(entry.getValue().unwrapped()));
//        });
//
//        return sparkConf;
//    }


}
