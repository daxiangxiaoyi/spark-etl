package com.daxiang.digest.framework.spark;

import com.daxiang.digest.configuration.CheckResult;
import com.daxiang.digest.framework.RuntimeEnv;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.StreamingContext;

/**
 * @author zhaozhuo
 * @date 2020/5/1
 */
public class SparkEnvironment implements RuntimeEnv {
    private Config config = ConfigFactory.empty();
    private SparkSession sparkSession;
    private StreamingContext streamingContext;

    @Override
    public void setConfig(Config config) {
        this.config = config;
    }

    @Override
    public Config getConfig() {
        return this.config;

    }

    @Override
    public CheckResult checkConfig() {
        return new CheckResult(true, "");
    }

    @Override
    public void prepare(Boolean prepareEnv) {
        SparkConf sparkConf = createSparkConf();
        sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();

    }

    private void createStreamingContext() {
        SparkConf conf = sparkSession.sparkContext().getConf();
        Long duration = conf.getLong("spark.stream.batchDuration", 5);
        if (streamingContext == null) {
            streamingContext = new StreamingContext(sparkSession.sparkContext(), Durations.seconds(duration));
        }
    }

    public SparkSession getSparkSession() {
        return sparkSession;
    }

    public void setSparkSession(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    public StreamingContext getStreamingContext() {
        return streamingContext;
    }

    public void setStreamingContext(StreamingContext streamingContext) {
        this.streamingContext = streamingContext;
    }

    private SparkConf createSparkConf() {
        //StringUtils.
        SparkConf sparkConf = new SparkConf();

        config.entrySet().forEach(entry -> {
            sparkConf.set(entry.getKey(), ((ConfigValue)entry.getValue()).unwrapped().toString());
        });
        return sparkConf;
    }


}
