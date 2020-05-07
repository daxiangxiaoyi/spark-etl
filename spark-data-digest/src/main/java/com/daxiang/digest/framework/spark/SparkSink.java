package com.daxiang.digest.framework.spark;

import com.daxiang.digest.framework.Sink;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * @author zhaozhuo
 * @date 2020/5/1
 */
public interface SparkSink<OUT> extends Sink<SparkEnvironment> {


     OUT output(Dataset<Row> dataset, SparkEnvironment env);
}
