package com.daxiang.digest.transform;

import com.daxiang.digest.configuration.CheckResult;
import com.daxiang.digest.framework.spark.SparkEnvironment;
import com.daxiang.digest.framework.spark.batch.SparkBatchTransform;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

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
