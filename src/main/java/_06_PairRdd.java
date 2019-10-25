import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class _06_PairRdd {

    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("startingSpark");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> inputData = new ArrayList<>();
        inputData.add("WARN: Tuesday 4 September 0405");
        inputData.add("ERROR: Tuesday 4 September 0408");
        inputData.add("FATAL: Wednesday 5 September 1632");
        inputData.add("ERROR: Friday 7 September 1854");
        inputData.add("WARN: Saturday 8 September 1942");

        sc.parallelize(inputData)
                .mapToPair(rawValue -> new Tuple2<>(rawValue.split(": ")[0], 1L))
                .reduceByKey(Long::sum)
                .foreach(tuple -> System.out.println(String.format("%s = %d", tuple._1, tuple._2)));

        sc.stop();

    }

}
