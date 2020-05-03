package com.ke.spider.digest.sink.batch;

import com.ke.spider.digest.configuration.CheckResult;
import com.ke.spider.digest.configuration.TypesafeConfigUtils;
import com.ke.spider.digest.framework.spark.SparkEnvironment;
import com.ke.spider.digest.framework.spark.batch.SparkBatchSink;
import com.ke.spider.digest.utils.FileUtils;
import com.ke.spider.digest.utils.StringTemplate;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.val;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.calcite.linq4j.tree.ExpressionType.Try;

/**
 * @author zhaozhuo
 * @date 2020/5/2
 */
public class BaseFileSink extends SparkBatchSink {
    @Override
    public Void output(Dataset<Row> dataset, SparkEnvironment env) {
        return null;
    }

    @Override
    public void process(Dataset<Row> df) {

    }

    @Override
    public CheckResult checkConfig() {
        return null;
    }
    protected CheckResult checkConfigImpl(List<String> allowedURISchema) {
        CheckResult checkResult;
        if (config.hasPath("path") && !config.getString("path").trim().isEmpty()) {
            String dir = config.getString("path");
            if (dir.startsWith("/") || FileUtils.uriInAllowedSchema(dir, allowedURISchema)) {
                checkResult=new CheckResult(true, "");
            } else {
                checkResult=new CheckResult(false, "invalid path URI, please set the following allowed schemas: " + String.join(",", allowedURISchema));

            }
        } else {
            checkResult=new CheckResult(false, "please specify [path] as non-empty string");
        }

        return checkResult;
    }
    @Override
    public void prepare(SparkEnvironment prepareEnv) {
        Map<String, Object> map = new HashMap<>();
        map.put("partition_by", Arrays.asList());
        map.put("save_mode", "error");
        map.put("serializer", "json");
        map.put("path_time_format", "yyyyMMddHHmmss");
        Config defaultConfig = ConfigFactory.parseMap(map);

        config = config.withFallback(defaultConfig);
    }

    protected void outputImpl(Dataset<Row> df, String defaultUriSchema){

        DataFrameWriter<Row> writer = df.write().mode(config.getString("save_mode"));
        if(config.getStringList("partition_by").size() != 0){
            List<String> partitionKeys = config.getStringList("partition_by");
            writer.partitionBy( partitionKeys.toArray(new String[partitionKeys.size()]));
        }

      try {
          Config configs=TypesafeConfigUtils.extractSubConfigThrowable(config, "options.", false);
          Map<String, String> map = new HashMap<>();
          configs.entrySet().forEach(entry -> {
              map.put(entry.getKey(), entry.getValue().toString());
          });
      }catch(Exception e){
          e.printStackTrace();
      }


        String path = buildPathWithDefaultSchema(config.getString("path"), defaultUriSchema);
        path = StringTemplate.substitute(path, config.getString("path_time_format"));
        String format = config.getString("serializer");
        FileUtils.write(writer, path, format);

    }



    protected String buildPathWithDefaultSchema(String uri, String defaultUriSchema){
        String path;
        if(uri.startsWith("/")){
            path=defaultUriSchema+uri;
        }else {
            path=uri;
        }

        return path;
    }
}
