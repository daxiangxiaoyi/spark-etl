package com.ke.spider.digest.framework.spark;

import com.ke.spider.digest.framework.Source;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * @author zhaozhuo
 * @date 2020/5/1
 */
public interface SparkSource<Data> extends Source<SparkEnvironment>{


     Data getData(SparkEnvironment env);
}
