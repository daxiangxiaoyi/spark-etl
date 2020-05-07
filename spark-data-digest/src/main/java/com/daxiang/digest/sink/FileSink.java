package com.daxiang.digest.sink;

import com.daxiang.digest.sink.batch.BaseFileSink;
import com.daxiang.digest.configuration.CheckResult;
import com.daxiang.digest.framework.spark.SparkEnvironment;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Arrays;

/**
 * @author zhaozhuo
 * @date 2020/5/2
 */
public class FileSink extends BaseFileSink {

    @Override
    public CheckResult checkConfig() {
        return checkConfigImpl(Arrays.asList("file://"));
    }

    @Override
    public Void output(Dataset<Row> data, SparkEnvironment env) {
        outputImpl(data, "file://");
        return null;
    }


}
