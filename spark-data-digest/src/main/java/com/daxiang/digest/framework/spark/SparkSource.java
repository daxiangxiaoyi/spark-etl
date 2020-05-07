package com.daxiang.digest.framework.spark;

import com.daxiang.digest.framework.Source;

/**
 * @author zhaozhuo
 * @date 2020/5/1
 */
public interface SparkSource<Data> extends Source<SparkEnvironment> {


     Data getData(SparkEnvironment env);
}
