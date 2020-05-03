package com.ke.spider.digest.framework;

import com.ke.spider.digest.configuration.CheckResult;
import com.typesafe.config.Config;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;

/**
 * @author zhaozhuo
 * @date 2020/4/29
 */
public interface Plugin<T> extends Serializable {
    String RESULT_TABLE_NAME = "result_table_name";
    String SOURCE_TABLE_NAME = "source_table_name";

    void setConfig(Config config);

    Config getConfig();

    CheckResult checkConfig();

    void prepare(T prepareEnv);

}
