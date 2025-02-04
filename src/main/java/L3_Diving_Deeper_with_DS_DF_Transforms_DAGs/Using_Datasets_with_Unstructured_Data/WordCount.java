package L3_Diving_Deeper_with_DS_DF_Transforms_DAGs.Using_Datasets_with_Unstructured_Data;

import L3_Diving_Deeper_with_DS_DF_Transforms_DAGs.Using_Datasets_with_Unstructured_Data.mappers.LineMapper;
import org.apache.spark.sql.*;
import org.apache.spark.sql.SparkSession;

public class WordCount {

    public void start(){

        String boringWords = "( 'a', 'b', 'c', 'and', 'dave', 'max', 'song', 'birt', 'damn', 'dove', 'pull', 'car', '[', '[]', ' ')";

        SparkSession spark = SparkSession.builder()
                .appName("unstructured text to flatmap")
                .master("local")
                .getOrCreate();

        String filename = "src/main/resources/L3/shakespeare.txt";

        Dataset<Row> df = spark.read().format("text").load(filename);

        df.show(1);

        Dataset<String> wordsDs = df.flatMap(new LineMapper(), Encoders.STRING());
        Dataset<Row> wordsDf = wordsDs.toDF();
        Dataset<Row> wordsAgg = wordsDf.groupBy("value").count();
        Dataset<Row> wordsAggDesc = wordsAgg.orderBy(wordsAgg.col("count").desc());
        Dataset<Row> filteredAggWordDesc = wordsAggDesc.filter("value not in " + boringWords);
        filteredAggWordDesc.show(10);

    }
}
