package L2_Spark_Java_Dataset_API.Real_World_Examples;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.jetbrains.annotations.NotNull;

import static org.apache.spark.sql.functions.*;

public class Application {

    public static void main(String[] args){

        SparkSession spark = new SparkSession.Builder()
                .appName("Data Transform")
                .master("local")
                .getOrCreate();

        Dataset<Row> durham_parks = spark.read()
                .format("json")
                .option("multiline", true)
                .load("src/main/resources/L2/durham-parks.json");

        Dataset<Row> philadelphia_recreations = spark.read()
                .format("csv")
                .option("header", true)
                .load("src/main/resources/L2/philadelphia_recreations.csv");

        Dataset<Row> students = spark.read()
                .format("csv")
                .option("header", true)
                .load("src/main/resources/L2/students.csv");


        System.out.println("Done loading files!");

        Dataset<Row> durham_new = buildDurhamDataset(durham_parks);

        durham_new.show(3);

    }

    public static Dataset<Row> buildDurhamDataset(@NotNull Dataset<Row> df) {
        Dataset<Row> durham;
        durham = df.withColumn(
                "park_id",
                concat(
                        df.col("datasetId"),
                        lit("_"),
                        df.col("fields.objectid"),
                        lit("_"),
                        lit("Durham")
                        )
                )
                .withColumn("park_name", df.col("fields.park_name"))
                .withColumn("city", lit("Durham"))
                .withColumn("has_playground", df.col("fields.playground"))
                .withColumn("zipcode", df.col("fields.zip"))
                .withColumn("land_in_acres", df.col("fields.acres"))
                .withColumn("geoX", df.col("geometry.coordinates").getItem(0))
                .withColumn("geoY", df.col("geometry.coordinates").getItem(1))
                .drop("fields")
                .drop("geometry")
                .drop("record_timestamp")
                .drop("recordid")
                .drop("datasetid");

        return durham;
    }
}
