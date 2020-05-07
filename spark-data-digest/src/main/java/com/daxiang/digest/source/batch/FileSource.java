package com.daxiang.digest.source.batch;

import com.daxiang.digest.configuration.CheckResult;
import com.daxiang.digest.configuration.TypesafeConfigUtils;
import com.daxiang.digest.framework.spark.batch.SparkBatchSource;
import com.daxiang.digest.utils.FileUtils;
import com.daxiang.digest.utils.StringTemplate;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author zhaozhuo
 * @date 2020/4/30
 */
public class FileSource extends SparkBatchSource {
    Config config = ConfigFactory.empty();



    @Override
    public void process(Dataset df) {

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
        return null;
    }



    protected Map<Boolean, String> checkConfigImpl(List<String> allowedURISchema) {
        Map<Boolean, String> map = new HashMap<>();
        if (config.hasPath("path") && config.getString("path").trim().isEmpty()) {
            String dir = config.getString("path");
            if (dir.startsWith("/") || FileUtils.uriInAllowedSchema(dir, allowedURISchema)) {
                map.put(true, "");
            } else {
                map.put(false, "invalid path URI, please set the following allowed schemas: " + String.join(",", allowedURISchema));

            }
        } else {
            map.put(false, "please specify [path] as non-empty string");
        }

        return map;
    }




    public void processImpl(Dataset<Row> df, String defaultUriSchema) {
        DataFrameWriter<Row> writer = fileWriter(df);
        String path = buildPathWithDefaultSchema(config.getString("path"), defaultUriSchema);
        path = StringTemplate.substitute(path, config.getString("path_time_format"));
        String format = config.getString("serializer");
        FileUtils.write(writer, path, format);

    }




    protected String buildPathWithDefaultSchema(String uri, String defaultUriSchema) {
        String path;
        if (uri.startsWith("/")) {
            path = defaultUriSchema + uri;
        } else {
            path = uri;
        }
        return path;
    }

    protected DataFrameWriter<Row> fileWriter(Dataset<Row> df) {
        DataFrameWriter<Row> writer = df.write().mode(config.getString("save_mode"));

        if (!config.getStringList("partition_by").isEmpty()) {
            List<String> partitionKeys = config.getStringList("partition_by");
            writer.partitionBy(String.valueOf(partitionKeys));
        }

        try {
            Config options = TypesafeConfigUtils.extractSubConfig(config, "options", false);
            Map<String, String> map = new HashMap<>();
            options.entrySet().forEach(entry -> {
                map.put(entry.getKey(), entry.getValue().toString());
            });
        } catch (Exception e) {
            e.printStackTrace();
        }

        return writer;

    }
}
