package com.ke.spider.digest.framework;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.expressions.UserDefinedFunction;

import java.util.List;
import java.util.Map;

/**
 * @author zhaozhuo
 * @date 2020/4/29
 */
public interface Transform <T extends RuntimeEnv> extends Plugin<T>{


//    List<Map<String, UserDefinedFunction>> getUdfList();
//
//
//    List<Map<String, UserDefinedAggregateFunction>> getUdafList();
}
