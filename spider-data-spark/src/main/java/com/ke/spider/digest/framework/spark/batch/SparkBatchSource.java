package com.ke.spider.digest.framework.spark.batch;

import com.ke.spider.digest.configuration.CheckResult;
import com.ke.spider.digest.framework.spark.SparkEnvironment;
import com.ke.spider.digest.framework.spark.SparkSource;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * @author zhaozhuo
 * @date 2020/5/1
 */
public  class SparkBatchSource implements SparkSource<Dataset<Row>> {

    protected Config config= ConfigFactory.empty();

    @Override
    public void setConfig(Config config) {
        this.config = config;
    }

    @Override
    public Config getConfig() {
        return config;
    }
    @Override
    public Dataset<Row> getData(SparkEnvironment env) {
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
}
