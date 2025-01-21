package L1_Introduction.project1_simpledemo;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Application {

    public static void main(String args[]) throws InterruptedException {

        // Create a session
        SparkSession spark = new SparkSession.Builder()
                .appName("CSV to Console")
                .master("local")
                .getOrCreate();

        // get data
        Dataset<Row> df = spark.read().format("csv")
                .option("header", true)
                .load("src/main/resources/name_and_comments.txt");

		df.show(3);

        // Transformation
        df = df.withColumn("full_name",
                        concat(df.col("last_name"), lit(", "), df.col("first_name")))
                .filter(df.col("comment").rlike("\\d+"))
                .orderBy(df.col("last_name").asc());

        df.show();

        Dataset<Row> df2 = spark.read().format("csv")
                .option("header", true)
                .load("src/main/resources/sample_data.csv");

        df2.select("ID", "Name", "Salary", "Department")
                .where("Salary > 50000")
                .show();
    }
}