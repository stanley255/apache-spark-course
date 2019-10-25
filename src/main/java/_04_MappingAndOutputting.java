import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

public class _04_MappingAndOutputting {

    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("startingSpark");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> inputData = new ArrayList<>();
        inputData.add(35);
        inputData.add(12);
        inputData.add(90);
        inputData.add(20);

        JavaRDD<Integer> myRdd =  sc.parallelize(inputData);

        Integer result = myRdd.reduce(Integer::sum);

        System.out.println(String.format("Total sum of myRdd elements is: %s",result));

        JavaRDD<Double> sqrtRdd = myRdd.map(Math::sqrt);

        sqrtRdd.collect().forEach(System.out::println);

        sc.close();
    }

}
