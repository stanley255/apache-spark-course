package _44_SparkStreaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

public class LogStreamAnalysis {

    public static void main(String[] args) throws InterruptedException {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("startingSpark");

        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(3));

        JavaReceiverInputDStream<String> inputData = sc.socketTextStream("localhost",8989);
        // Series of RDDs
        JavaDStream<String> results = inputData.map(item -> item);
        results.map(rawLogMessage -> rawLogMessage.split(",")[0])
                .mapToPair(logLevel -> new Tuple2<>(logLevel, 1L))
                .reduceByKeyAndWindow(Long::sum, Durations.minutes(1))
                .print();

        sc.start();
        sc.awaitTermination();

    }

}
