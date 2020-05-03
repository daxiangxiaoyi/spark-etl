package com.ke.spider.digest.source.batch;

import com.ke.spider.digest.configuration.CheckResult;
import com.ke.spider.digest.framework.RuntimeEnv;
import com.ke.spider.digest.framework.Source;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author zhaozhuo
 * @date 2020/4/30
 */
public class MysqlSource<K> {
//    private Boolean firstProcess = true;
//
//    private Config config = ConfigFactory.empty();
//
//    @Override
//    public Dataset<Row> getDataset(SparkSession spark) {
//        return null;
//    }
//
//    @Override
//    public void process(Dataset<Row> df) {
//        Properties prop = new java.util.Properties();
//        prop.setProperty("driver", "com.mysql.jdbc.Driver");
//        prop.setProperty("user", config.getString("user"));
//        prop.setProperty("password", config.getString("password"));
//
//        String saveMode = config.getString("save_mode");
//
//        if (firstProcess) {
//            df.write().mode(saveMode).jdbc(config.getString("url"), config.getString("table"), prop);
//            firstProcess = false;
//        } else if (saveMode == "overwrite") {
//            // actually user only want the first time overwrite in streaming(generating multiple dataframe)
//            df.write().mode(SaveMode.Append).jdbc(config.getString("url"), config.getString("table"), prop);
//        } else {
//            df.write().mode(saveMode).jdbc(config.getString("url"), config.getString("table"), prop);
//        }
//    }
//
//
//    @Override
//    public void setConfig(Config config) {
//        this.config = config;
//    }
//
//    @Override
//    public Config getConfig() {
//        return this.config;
//    }
//
//
//
//    @Override
//    public CheckResult checkConfig() {
//
//        List<String> requiredOptions = Arrays.asList("url", "table", "user", "password");
//        Map<Boolean, String> nonExistsOptions = new HashMap<>();
//        requiredOptions.stream().collect(Collectors.toMap(optionName -> optionName, optionName -> config.hasPath(optionName)))
//                .forEach((key, value) -> {
//                    if (!value) {
//                        nonExistsOptions.put(false, key);
//                    }
//                });
//
//
//        if (nonExistsOptions.size() == 0) {
//
//            List<String>  saveModeAllowedValues = Arrays.asList("overwrite", "append", "ignore", "error");
//
//            if (!config.hasPath("save_mode") || saveModeAllowedValues.contains(config.getString("save_mode"))) {
//                nonExistsOptions.put(true, "");
//            } else {
//                nonExistsOptions.put(false, "wrong value of [save_mode], allowed values: " + String.join(",",saveModeAllowedValues));
//            }
//
//        } else {
//            nonExistsOptions.put(false, "please specify " + String.join(",",nonExistsOptions.values())+ "as non-empty string");
//        }
//        return new CheckResult(true,"");
//    }
//
//    @Override
//    public void prepare(Object prepareEnv) {
//
//    }



}
