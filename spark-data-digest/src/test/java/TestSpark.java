import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

/**
 * @author zhaozhuo
 * @date 2020/5/3
 */
public class TestSpark {

    public static void main(String[] args) {
        SparkConf sparkConf=new SparkConf();
        sparkConf.set("spark.master","local");
        sparkConf.set(" spark.executor.memory","1g");
        SparkSession spark= SparkSession.builder().config(sparkConf).getOrCreate();
        spark.read().textFile("D:\\Users\\zz001\\SourceCode\\spark-data-etl\\spark-data-spark\\src\\main\\resources\\example.conf").show();
    }
}
