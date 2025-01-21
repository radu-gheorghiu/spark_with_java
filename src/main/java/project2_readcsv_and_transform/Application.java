package project2_readcsv_and_transform;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

import project3_write_to_sqlite.SQLiteWriter;

import java.sql.SQLException;
import java.util.Properties;

public class Application {
    public static void main(String args[]) throws SQLException {

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

        SQLiteWriter writer = SQLiteWriter.getInstance("data\\sparkwriterapp.db");

        String dbConnectionUrl = "jdbc:sqlite:data\\sparkwriterapp.db";
        Properties prop = new Properties();
        prop.setProperty("driver", "org.sqlite.JDBC");

        df.write()
                .mode(SaveMode.Overwrite)
                .jdbc(dbConnectionUrl, "project2", prop);

    }
}