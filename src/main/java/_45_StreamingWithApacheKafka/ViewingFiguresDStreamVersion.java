package _45_StreamingWithApacheKafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.*;

public class ViewingFiguresDStreamVersion {

    public static void main(String[] args) throws InterruptedException {
//        System.setProperty("hadoop.home.dir", "c:/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("viewingFigures");

        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(1));

        Collection topics = Collections.singletonList("viewrecords");

        Map<String, Object> params = new HashMap<>();
        params.put("bootstrap.servers", "localhost:9092");
        params.put("key.deserializer", StringDeserializer.class);
        params.put("value.deserializer", StringDeserializer.class);
        params.put("group.id","spark-group");
        params.put("auto.offset.reset", "latest");
        params.put("enable.auto.commit", false);


        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(sc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, params)
        );

        JavaPairDStream<Long, String> results = stream.mapToPair(item -> new Tuple2<>(item.value(), 5L))
                .reduceByKeyAndWindow(Long::sum, Durations.minutes(60), Durations.minutes(1))
                .mapToPair(Tuple2::swap)
                .transformToPair(rdd -> rdd.sortByKey(false));

        results.print(50);

        sc.start();
        sc.awaitTermination();


    }

}
