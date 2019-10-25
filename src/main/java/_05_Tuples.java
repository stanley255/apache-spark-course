import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class _05_Tuples {

    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("startingSpark");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> inputData = new ArrayList<>();
        inputData.add(35);
        inputData.add(12);
        inputData.add(90);
        inputData.add(20);

        JavaRDD<Integer> originalIntegers =  sc.parallelize(inputData);
        JavaRDD<Tuple2<Integer, Double>> sqrtRdd = originalIntegers.map(x->new Tuple2<>(x,Math.sqrt(x)));

        sqrtRdd.collect().forEach(x->System.out.println(x._1+"->"+x._2));

    }

}
