package L2_Spark_Java_Dataset_API.Real_World_Examples;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

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

    }
}
