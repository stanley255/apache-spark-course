import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class _09_KeywordRankingPractical {

    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("startingSpark");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> initialRdd = sc.textFile("src/main/resources/subtitles/input.txt");

        initialRdd
                .map(sentence -> sentence.toLowerCase().replaceAll("[^a-zA-Z\\s]", " "))
                .flatMap(sentence-> Arrays.asList(sentence.split(" ")).iterator())
                .filter(sentence -> sentence.trim().length() > 0)
                .filter(Util::isNotBoring)
                .mapToPair(word->new Tuple2<>(word, 1L))
                .reduceByKey(Long::sum)
                .mapToPair(Tuple2::swap)
                .sortByKey(false)
                .take(20)
                .forEach(x-> System.out.println(String.format("%d : %s", x._1, x._2)));

        sc.close();
    }

}
