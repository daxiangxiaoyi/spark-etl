package com.ke.spider.digest.framework;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @author zhaozhuo
 * @date 2020/4/29
 */
public interface Source <T extends RuntimeEnv> extends Plugin<T>  {


   public void process(Dataset<Row> df);
}
