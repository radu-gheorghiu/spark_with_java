package L3_Diving_Deeper_with_DS_DF_Transforms_DAGs.Aggregate_Transformations_Join_Assignment;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Application {
    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("Aggregate Transformations")
                .master("local")
                .getOrCreate();

        String customers_file = "src/main/resources/L3/customers.csv";

        Dataset<Row> customersDf = spark.read().format("csv")
                .option("inferSchema", true)
                .option("header", true)
                .load(customers_file);

        String products_file = "src/main/resources/L3/products.csv";

    }
}
