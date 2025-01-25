package L3_Diving_Deeper_with_DS_DF_Transforms_DAGs.Using_Datasets_with_Unstructured_Data;

import org.apache.spark.sql.*;
import org.apache.spark.sql.SparkSession;

public class WordCount {

    public void start(){

        String boringWords = "( 'a', 'b', 'c', 'and', 'dave', 'max', 'song', 'birt', 'damn', 'dove', 'pull', 'car'";

        SparkSession spark = SparkSession.builder()
                .appName("unstructured text to flatmap")
                .master("local")
                .getOrCreate();

        String filename = "src/main/resources/L3/shakespeare.txt";

        Dataset<Row> df = spark.read().format("text").load(filename);

        df.show();

    }
}
