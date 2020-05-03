package com.ke.spider.digest.framework;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * @author zhaozhuo
 * @date 2020/4/29
 */
public interface Sink <T extends RuntimeEnv> extends Plugin<T>{

   void process(Dataset<Row> df);
}
