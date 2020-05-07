package com.daxiang.digest.utils;

import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Row;

import java.util.List;

/**
 * @author zhaozhuo
 * @date 2020/5/2
 */
public class FileUtils {
    public static void write(DataFrameWriter<Row> writer, String path, String format) {
        switch (format) {
            case "csv":
                writer.csv(path);
                break;
            case "json":
                writer.json(path);
                break;
            case "parquet":
                writer.parquet(path);
                break;
            case "text":
                writer.text(path);
                break;
            case "orc":
                writer.orc(path);
                break;
            default:
                writer.format(format).save(path);
        }
    }

    public static Boolean uriInAllowedSchema(String uri, List<String> allowedURISchema) {
        return allowedURISchema.stream().allMatch(s -> uri.startsWith(s));
    }
}
