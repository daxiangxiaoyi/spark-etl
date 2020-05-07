package com.daxiang.digest.framework.spark;

import com.daxiang.digest.framework.Transform;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * @author zhaozhuo
 * @date 2020/5/1
 */
public interface SparkTransform extends Transform<SparkEnvironment> {

     Dataset<Row> process(Dataset<Row> data, SparkEnvironment env);
}
