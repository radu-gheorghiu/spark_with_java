package L2_Spark_Java_Dataset_API.Converting_Between_Datasets_and_Dataframes;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

public class ArrayToDataset {
    public void start(){
        SparkSession spark = new SparkSession.Builder()
                .appName("Array To Dataset<String>")
                .master("local")
                .getOrCreate();

        String[] stringList = new String[]{"Banana", "Car", "Apple", "Dog", "Glass", "Computer", "Car", "Glass"};
        List<String> data = Arrays.asList(stringList);

        Dataset<String> ds = spark.createDataset(data, Encoders.STRING());

        ds.show();
        ds.printSchema();

        Dataset<Row> df = ds.groupBy("value").count();
        df.show();

        // another method to convert a Dataset to a Dataframe is to us .toDF();
        Dataset<Row> df2 = ds.toDF();

        // and converting it back to a Dataset
        Dataset<String> ds2 = df2.as(Encoders.STRING());
        ds2.show();
    }
}
