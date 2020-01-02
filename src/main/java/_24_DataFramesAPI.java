import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

public class _24_DataFramesAPI {

    @SuppressWarnings("resource")
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("testingSql")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///c:/tmp/")
                .getOrCreate();

        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/biglog.txt");

        dataset = dataset.select(col("level"),
                                 date_format(col("datetime"), "MMMM").alias("month"),
                                 date_format(col("datetime"), "M").alias("monthnum").cast(DataTypes.IntegerType))
                         .groupBy(col("level"), col("month"), col("monthnum")).count()
                         .orderBy(col("monthnum"), col("level"))
                         .drop(col("monthnum"));

        dataset.show(100);

        spark.close();
    }

}
