package L2_Spark_Java_Dataset_API;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class InferSchema {

    public void printSchema() {
        SparkSession spark = SparkSession.builder()
                .appName("read_csv_json")
                .master("local")
                .getOrCreate();

        Dataset<Row> df = spark.read().format("csv")
                .option("header", "true")
                .option("multiline", true)
                .option("sep", ";")
                .option("quote", "^")
                .option("dateFormat", "M/d/y")
                .option("inferSchema", true)
                .load("src/main/resources/amazonProducts.txt");

        System.out.println("Sample data");
        df.show(3);

        System.out.println("Dataframe schema");
        df.printSchema();
    }
}
