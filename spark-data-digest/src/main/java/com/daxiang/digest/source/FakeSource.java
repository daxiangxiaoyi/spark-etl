package com.daxiang.digest.source;

import com.daxiang.digest.configuration.CheckResult;
import com.daxiang.digest.framework.spark.SparkEnvironment;
import com.daxiang.digest.framework.spark.batch.SparkBatchSource;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zhaozhuo
 * @date 2020/5/1
 */
public class FakeSource extends SparkBatchSource {
    @Override
    public Dataset<Row> getData(SparkEnvironment env) {


        List<Row> s = new ArrayList<>();
        s.add(RowFactory.create("Hello garyelephant"));
        s.add(RowFactory.create("Hello rickyhuo"));
        s.add(RowFactory.create("Hello kid-xiong"));

        StructType schema = new StructType().add("raw_message", DataTypes.StringType);


        return env.getSparkSession().createDataset(s,RowEncoder.apply(schema));
    }



    @Override
    public void process(Dataset df) {

    }

    @Override
    public CheckResult checkConfig() {
        return new CheckResult(true, "");
    }




}
