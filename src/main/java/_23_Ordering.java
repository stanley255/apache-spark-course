import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class _23_Ordering {

    @SuppressWarnings("resource")
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("testingSql")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///c:/tmp/")
                .getOrCreate();

        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/biglog.txt");

        dataset.createOrReplaceTempView("logs");

        Dataset<Row> results = spark.sql("select date_format(datetime, 'MMMM') as month, level, count(1) as total " +
                "from logs group by level, month " +
                "order by cast(first(date_format(datetime, 'M')) as int), level");

        results.show(100);

        spark.close();
    }

}
