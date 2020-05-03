package com.ke.spider.digest.framework.spark;

import com.ke.spider.digest.framework.Sink;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * @author zhaozhuo
 * @date 2020/5/1
 */
public interface SparkSink<OUT> extends  Sink<SparkEnvironment>{


     OUT output(Dataset<Row> dataset, SparkEnvironment env);
}
