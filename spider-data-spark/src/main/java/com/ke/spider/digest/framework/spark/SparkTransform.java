package com.ke.spider.digest.framework.spark;

import com.ke.spider.digest.framework.Source;
import com.ke.spider.digest.framework.Transform;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * @author zhaozhuo
 * @date 2020/5/1
 */
public interface SparkTransform extends Transform<SparkEnvironment> {

     Dataset<Row> process(Dataset<Row> data, SparkEnvironment env);
}
