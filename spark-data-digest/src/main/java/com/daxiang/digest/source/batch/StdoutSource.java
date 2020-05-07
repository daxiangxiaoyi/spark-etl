package com.daxiang.digest.source.batch;

import com.daxiang.digest.configuration.CheckResult;
import com.daxiang.digest.framework.spark.batch.SparkBatchSource;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.sql.Dataset;

import java.util.Arrays;

/**
 * @author zhaozhuo
 * @date 2020/4/30
 */
public class StdoutSource extends SparkBatchSource {
    Config config = ConfigFactory.empty();



    @Override
    public void process(Dataset df) {
        Integer limit = config.getInt("limit");
        switch (config.getString("serializer")) {
            case "plain":
                if (limit == -1) {
                    df.show(Integer.MAX_VALUE, false);
                } else if (limit > 0) {
                    df.show(limit, false);
                }
                break;
            case "json":
                if (limit == -1) {
                    Arrays.asList(df.toJSON().take(Integer.MAX_VALUE)).forEach(s -> System.out.println());
                } else if (limit > 0) {
                    Arrays.asList(df.toJSON().take(limit)).forEach(s -> System.out.println());

                }
        }

    }

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
        CheckResult checkResult;
        if (!config.hasPath("limit") || (config.hasPath("limit") && config.getInt("limit") >= -1)) {
            checkResult=new CheckResult(true, "");
        } else {
            checkResult=new CheckResult(false, "please specify [limit] as Number[-1, " + Integer.MAX_VALUE + "]");
        }
        return checkResult;
    }



//    @Override
//    public void prepare(SparkSession sparkSession) {
//        Config defaultConfig = ConfigFactory.parseString("{\"limit\":100,\"serializer\":\"plain\"");
//
//        config = config.withFallback(defaultConfig);
//    }
}
