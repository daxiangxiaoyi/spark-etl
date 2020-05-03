package com.ke.spider.digest.source;

import com.ke.spider.digest.configuration.CheckResult;
import com.ke.spider.digest.configuration.TypesafeConfigUtils;
import com.ke.spider.digest.framework.spark.SparkEnvironment;
import com.ke.spider.digest.framework.spark.batch.SparkBatchSource;
import com.typesafe.config.Config;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author zhaozhuo
 * @date 2020/5/1
 */
public class JdbcSource extends SparkBatchSource {
    @Override
    public Dataset<Row> getData(SparkEnvironment env) {
        return jdbcReader(env.getSparkSession(), config.getString("driver")).load();
    }



    @Override
    public void process(Dataset<Row> df) {

    }

    @Override
    public CheckResult checkConfig() {
        CheckResult checkResult=null;
        List<String> requiredOptions = Arrays.asList("url", "table", "user", "password", "driver");
        Map<Boolean, String> nonExistsOptions = new HashMap<>();
        requiredOptions.stream().collect(Collectors.toMap(optionName -> optionName, optionName -> config.hasPath(optionName)))
                .forEach((key, value) -> {
                    if (!value) {
                        nonExistsOptions.put(false, key);
                    }
                });


        if (nonExistsOptions.isEmpty()) {
           checkResult= new CheckResult(true, "");
        } else {
            checkResult= new CheckResult(false,
                    "please specify " + nonExistsOptions);

        }
        return checkResult;
    }

    @Override
    public void prepare(SparkEnvironment prepareEnv) {

    }
    protected DataFrameReader jdbcReader(SparkSession sparkSession, String driver) {

        DataFrameReader reader = sparkSession.read()
                .format("jdbc")
                .option("url", config.getString("url"))
                .option("dbtable", config.getString("table"))
                .option("user", config.getString("user"))
                .option("password", config.getString("password"))
                .option("driver", driver);
        try {
            Config options = TypesafeConfigUtils.extractSubConfig(config, "jdbc", false);
            Map<String, String> optionMap = new HashMap<>();
            for (Map.Entry entry : options.entrySet()) {
                optionMap.put(entry.getKey().toString(), entry.getValue().toString());


            }
            reader.options(optionMap);
        } catch (Exception e) {
            e.printStackTrace();
        }


        return reader;
    }

}
