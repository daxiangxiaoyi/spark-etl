package com.ke.spider.digest.transform;

import com.ke.spider.digest.configuration.CheckResult;
import com.ke.spider.digest.framework.spark.SparkTransform;
import com.ke.spider.digest.framework.spark.SparkEnvironment;
import com.ke.spider.digest.framework.spark.batch.SparkBatchTransform;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.expressions.UserDefinedFunction;

import java.util.List;
import java.util.Map;

/**
 * @author zhaozhuo
 * @date 2020/5/2
 */
public class SqlTransform extends SparkBatchTransform {
    @Override
    public Dataset<Row> process(Dataset<Row> data, SparkEnvironment env) {
        return env.getSparkSession().sql(config.getString("sql"));
    }


    @Override
    public CheckResult checkConfig() {
        CheckResult checkResult;
        if(config.hasPath("sql")){
            checkResult=new CheckResult(true,"");
        }else {
            checkResult=new CheckResult(false,"please specify [sql]");
        }
        return checkResult;
    }

    @Override
    public void prepare(SparkEnvironment prepareEnv) {

    }
}
