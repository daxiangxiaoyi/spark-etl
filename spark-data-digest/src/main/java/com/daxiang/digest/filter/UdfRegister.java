package com.daxiang.digest.filter;

import org.apache.spark.sql.SparkSession;

import static org.apache.commons.math3.analysis.FunctionUtils.add;

/**
 * @author zhaozhuo
 * @date 2020/4/29
 */
public class UdfRegister<T> {
    static int udfCount = 0;
    static int udafCount = 0;

    public static void findAndRegisterUdfs(SparkSession spark) {


//        ServiceLoader.load(Transform.class).forEach(f -> {
//            f.getUdafList().forEach(udaf -> {
//                String name = null;
//                UserDefinedAggregateFunction obj = null;
//                for (Map.Entry entry : udaf.entrySet()) {
//                    name = entry.getKey().toString();
//                    obj = (UserDefinedAggregateFunction) entry.getValue();
//                }
//                spark.udf().register(name, obj);
//                udfCount += udfCount + 1;
//
//            });
//            f.getUdfList().forEach(udf -> {
//                String name = null;
//                UserDefinedFunction obj = null;
//                for (Map.Entry entry : udf.entrySet()) {
//                    name = entry.getKey().toString();
//                    obj = (UserDefinedFunction) entry.getValue();
//                }
//                spark.udf().register(name, obj);
//                udafCount += 1;
//            });
//        });

    }

}
