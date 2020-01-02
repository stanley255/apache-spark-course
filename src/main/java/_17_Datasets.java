import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class _17_Datasets {

    @SuppressWarnings("resource")
    public static void main(String[] args)
    {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("testingSql")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///c:/tmp/")
                .getOrCreate();

        // Like table of data
        Dataset<Row> dataset = spark.read()
                .option("header", true)
                .csv("src/main/resources/exams/students.csv");

        dataset.show();

        long numberOfRows = dataset.count();

        System.out.println(String.format("There are %d records", numberOfRows));

        Row firstRow = dataset.first();
        // It is possible to use get() with index as well as getAs() with column name
        String subject = firstRow.getAs("subject").toString();
        int year = Integer.parseInt(firstRow.getAs("year"));
        System.out.println(subject);
        System.out.println(year);

        Dataset<Row> modernArtResultsSQLWay = dataset.filter("subject = 'Modern Art' AND year >= 2007");

        Dataset<Row> modernArtResultsLambdaWay = dataset.filter((FilterFunction<Row>) row -> row.getAs("subject").equals("Modern Art")
                                                                                             && Integer.parseInt(row.getAs("year")) >= 2007);

        Dataset<Row> modernArtResults = dataset.filter(col("subject").equalTo("Modern Art")
                                                       .and(col("year").geq(2007)));
        

        modernArtResults.show();

        spark.close();
    }

}
