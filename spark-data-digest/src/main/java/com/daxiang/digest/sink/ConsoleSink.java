package com.daxiang.digest.sink;

import com.daxiang.digest.configuration.CheckResult;
import com.daxiang.digest.framework.spark.SparkEnvironment;
import com.daxiang.digest.framework.spark.batch.SparkBatchSink;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @author zhaozhuo
 * @date 2020/5/2
 */
public class ConsoleSink extends SparkBatchSink {
    @Override
    public Void output(Dataset<Row> df, SparkEnvironment env) {
        int limit = config.getInt("limit");
        if(config.hasPath("serializer")){
            switch (config.getString("serializer")){
                case "plain":
                    if (limit == -1) {
                        df.show(Integer.MAX_VALUE, false);
                    } else if (limit > 0) {
                        df.show(limit, false);
                    }
                    break;
                case "json":
                    if (limit == -1) {
                        Arrays.asList(df.toJSON().take(Integer.MAX_VALUE)).forEach(s-> System.out.println(s));

                    } else if (limit > 0) {
                        Arrays.asList(df.toJSON().take(limit)).forEach(s-> System.out.println(s));
                    }
            }
        }
        return null;
    }

    @Override
    public void process(Dataset<Row> df) {

    }

    @Override
    public CheckResult checkConfig() {
        CheckResult checkResult;
        if (!config.hasPath("limit") || (config.hasPath("limit") && config.getInt("limit") >= -1)) {
            checkResult = new CheckResult(true, "");
        } else {
            checkResult = new CheckResult(false, "please specify [limit] as Number[-1, " + Integer.MAX_VALUE + "]");
        }


        return checkResult;
    }

    @Override
    public void prepare(SparkEnvironment prepareEnv) {
        Map<String, Object> map = new HashMap<>();
        map.put("limit", 100);
        map.put("serializer", "plain");
        Config defaultConfig = ConfigFactory.parseMap(map);
        config = config.withFallback(defaultConfig);
    }
}
