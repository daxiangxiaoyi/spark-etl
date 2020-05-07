package com.daxiang.digest.framework.spark.batch;

import com.daxiang.digest.configuration.CheckResult;
import com.daxiang.digest.framework.spark.SparkSink;
import com.daxiang.digest.framework.spark.SparkEnvironment;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * @author zhaozhuo
 * @date 2020/5/1
 */
public class SparkBatchSink implements SparkSink<Void>  {
    protected Config config = ConfigFactory.empty();
    @Override
    public Void output(Dataset<Row> dataset, SparkEnvironment env) {
        return null;
    }

    @Override
    public void process(Dataset<Row> df) {

    }


    @Override
    public CheckResult checkConfig() {
        return null;
    }

    @Override
    public void prepare(SparkEnvironment prepareEnv) {

    }




    @Override
    public void setConfig(Config config) {
        this.config = config;
    }


    @Override
    public Config getConfig() {
        return config;
    }
}
