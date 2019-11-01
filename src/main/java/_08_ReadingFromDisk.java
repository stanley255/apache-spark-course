import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class _08_ReadingFromDisk {

    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("startingSpark");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> initialRdd = sc.textFile("src/main/resources/subtitles/input.txt");

        initialRdd
                .flatMap(sentence-> Arrays.asList(sentence.split(" ")).iterator())
                .collect()
                .forEach(System.out::println);

        sc.close();
    }

}
