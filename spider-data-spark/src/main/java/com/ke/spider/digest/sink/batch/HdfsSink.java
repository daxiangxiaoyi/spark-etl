package com.ke.spider.digest.sink.batch;

import com.ke.spider.digest.configuration.CheckResult;
import com.ke.spider.digest.framework.spark.SparkEnvironment;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Arrays;

/**
 * @author zhaozhuo
 * @date 2020/5/2
 */
public class HdfsSink extends BaseFileSink {

    @Override
    public CheckResult checkConfig() {
        return checkConfigImpl(Arrays.asList("hdfs://"));
    }

    @Override
    public Void output(Dataset<Row> data, SparkEnvironment env) {
        outputImpl(data, "hdfs://");
        return null;
    }
}
