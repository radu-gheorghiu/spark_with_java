package project2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

public class Application {
    public static void main(String args[]){

        // create a session
        SparkSession spark = new SparkSession.Builder()
                .appName("read csv to sqlite")
                .master("local")
                .getOrCreate();

        // get data
        Dataset<Row> df = spark.read().format("csv")
                .option("header", "true")
                .load("src/main/resources/sample_data.csv");

        // show the contents of the dataframe
        df.show();

        // add new columns with transformations
        df = df.withColumn("Years until retirement", lit(60).minus(df.col("Age")));
        df = df.withColumn("Full Name",
                concat(
                        df.col("Name"),
                        lit(", "),
                        df.col("Last Name")
                ));

        df.show();

        // filter rows
        df.filter(
                df.col("Full Name").rlike("e")
        ).show();
    }
}