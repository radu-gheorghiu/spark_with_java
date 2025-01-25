package L2_Spark_Java_Dataset_API.Real_World_Examples;

import org.apache.spark.Partition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.jetbrains.annotations.NotNull;

import javax.xml.crypto.Data;

import java.util.Arrays;

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
        Dataset<Row> philadelphia_new = buildPhiladelphia(philadelphia_recreations);

        durham_new.show(3);
        System.out.println("In DURHAM we have " + durham_new.count() + " rows;");
        System.out.println("============================");

        philadelphia_new.show(3);
        System.out.println("In PHILADELPHIA We have " + philadelphia_new.count() + " rows;");
        System.out.println("============================");

        Dataset<Row> durphil = combineDataframes(philadelphia_new, durham_new);
        durphil.show(3);
        System.out.println("In TOTAL We have " + durphil.count() + " rows;");

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

    public static Dataset<Row> buildPhiladelphia(@NotNull Dataset<Row> df) {
        Dataset<Row> philadelphia;

        // there are multiple ways of filtering, using spark Java API methods which are chained together
        // philadelphia = df.filter(lower(df.col("USE_")).like("%park%"));

        // or just using regular SQL code in your filter
        philadelphia = df.filter("lower(USE_) like '%park%'");
        philadelphia = philadelphia.withColumn("park_id",concat(lit("phil_"), df.col("OBJECTID")))
                .withColumnRenamed("ASSET_NAME", "park_name")
                .withColumn("city", lit("Philadelphia"))
                .withColumn("has_playground", lit("UNKNOWN"))
                .withColumnRenamed("ZIPCODE","zipcode")
                .withColumnRenamed("ACREAGE", "land_in_acres")
                .withColumn("geoX", lit("UNKNOWN"))
                .withColumn("geoY", lit("UNKNOWN"));

        philadelphia = philadelphia.select("park_id", "park_name", "city", "has_playground", "zipcode", "land_in_acres", "geoX", "geoY");

        return philadelphia;
    }

    public static Dataset<Row> combineDataframes(Dataset<Row> df1, Dataset<Row> df2) {
        Dataset<Row> combinedDf;
        combinedDf = df1.unionByName(df2);

        combinedDf = combinedDf.repartition(1);

        Partition[] partitions = combinedDf.rdd().partitions();

        System.out.println("Total partitions: " + partitions.length);

        return combinedDf;
    }
}
