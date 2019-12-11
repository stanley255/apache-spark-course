import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class _16_SparkSQL {

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

		spark.close();
	}

}
