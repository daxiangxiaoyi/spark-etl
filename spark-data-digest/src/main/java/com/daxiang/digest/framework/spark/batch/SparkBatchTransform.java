package com.daxiang.digest.framework.spark.batch;

import com.daxiang.digest.configuration.CheckResult;
import com.daxiang.digest.framework.spark.SparkEnvironment;
import com.daxiang.digest.framework.spark.SparkTransform;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * @author zhaozhuo
 * @date 2020/5/3
 */
public class SparkBatchTransform implements SparkTransform {
    protected Config config = ConfigFactory.empty();
    @Override
    public Dataset<Row> process(Dataset<Row> data, SparkEnvironment env) {
        return null;
    }


    @Override
    public void setConfig(Config config) {

    }

    @Override
    public Config getConfig() {
        return null;
    }

    @Override
    public CheckResult checkConfig() {
        return null;
    }

    @Override
    public void prepare(SparkEnvironment prepareEnv) {

    }
}
