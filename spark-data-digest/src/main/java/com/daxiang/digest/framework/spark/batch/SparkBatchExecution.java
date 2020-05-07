package com.daxiang.digest.framework.spark.batch;

import com.daxiang.digest.configuration.CheckResult;
import com.daxiang.digest.configuration.ConfigRuntimeException;
import com.daxiang.digest.framework.Execution;
import com.daxiang.digest.framework.spark.SparkSink;
import com.daxiang.digest.framework.spark.SparkSource;
import com.daxiang.digest.framework.spark.SparkTransform;
import com.daxiang.digest.framework.spark.SparkEnvironment;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;

/**
 * @author zhaozhuo
 * @date 2020/5/1
 */
public class SparkBatchExecution implements Execution<SparkBatchSource, SparkBatchTransform,SparkBatchSink> {
    private static String sourceTableName = "source_table_name";
    private static String resultTableName = "result_table_name";
    private Config config = ConfigFactory.empty();
    private SparkEnvironment environment;

    public SparkBatchExecution(SparkEnvironment environment) {
        this.environment = environment;
    }

    @Override
    public void start(List<SparkBatchSource> sources, List<SparkBatchTransform> transforms, List<SparkBatchSink> sinks) {

        sources.forEach(s -> {
            registerInputTempView(s, environment);
        });
        if (!sources.isEmpty()) {
            Dataset<Row> ds = sources.get(0).getData(environment);

            for (SparkTransform tf : transforms) {
                //强制转换
                Row[] rows=(Row[])ds.take(1);
                if (rows.length >0) {
                    ds = transformProcess(environment, tf, ds);
                    registerTransformTempView(tf, ds);
                }
            }

            for (SparkBatchSink s : sinks) {
                sinkProcess(environment, s, ds);
            }
        }
    }

    @Override
    public void setConfig(Config config) {
        this.config = config;
    }

    @Override
    public Config getConfig() {
        return this.config;
    }

    @Override
    public CheckResult checkConfig() {
        return new CheckResult(true, "");
    }

    @Override
    public void prepare(Void prepareEnv) {

    }




    private void registerTempView(String tableName, Dataset<Row> ds) {
        ds.createOrReplaceTempView(tableName);
    }

    private void registerInputTempView(SparkSource<Dataset<Row>> source, SparkEnvironment environment) {
        Config conf = source.getConfig();

        if (conf.hasPath(SparkBatchExecution.resultTableName)) {
            String tableName = conf.getString(SparkBatchExecution.resultTableName);
            registerTempView(tableName, source.getData(environment));
        } else {
            throw new ConfigRuntimeException("Plugin[" + source.getClass().getName() + "] must be registered as dataset/table, please set \"result_table_name\" config");
        }


    }

    private Dataset<Row> transformProcess(SparkEnvironment environment, SparkTransform transform, Dataset<Row> ds) {
        Config config = transform.getConfig();
        Dataset<Row> fromDs = null;
        if (config.hasPath(SparkBatchExecution.sourceTableName)) {
            String sourceTableName = config.getString(SparkBatchExecution.sourceTableName);
            fromDs = environment.getSparkSession().read().table(sourceTableName);
        } else {
            fromDs = ds;
        }

        return transform.process(fromDs, environment);
    }

    private void registerTransformTempView(SparkTransform plugin, Dataset<Row> ds) {
        Config config = plugin.getConfig();
        if (config.hasPath(SparkBatchExecution.resultTableName)) {
            String tableName = config.getString(SparkBatchExecution.resultTableName);
            registerTempView(tableName, ds);
        }
    }

    private void sinkProcess(SparkEnvironment environment, SparkSink sink, Dataset<Row> ds) {
        Config config = sink.getConfig();
        Dataset<Row> fromDs = null;
        if (config.hasPath(SparkBatchExecution.sourceTableName)) {
            String sourceTableName = config.getString(SparkBatchExecution.sourceTableName);
            fromDs = environment.getSparkSession().read().table(sourceTableName);
        } else {
            fromDs = ds;
        }

        sink.output(fromDs, environment);
    }


}
