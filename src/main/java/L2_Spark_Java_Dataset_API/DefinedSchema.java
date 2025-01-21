package L2_Spark_Java_Dataset_API;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import shapeless.DataT;

import javax.xml.crypto.Data;
import java.sql.Struct;

public class DefinedSchema {

    public void printDefinedSchema(){
        SparkSession spark = SparkSession.builder()
                .appName("read_csv_json")
                .master("local")
                .getOrCreate();

        StructType schema = DataTypes.createStructType(new StructField[]{
           DataTypes.createStructField("id", DataTypes.IntegerType, false),
           DataTypes.createStructField("product_id", DataTypes.IntegerType, false),
           DataTypes.createStructField("item_name", DataTypes.StringType, false),
           DataTypes.createStructField("published_on", DataTypes.DateType, false),
           DataTypes.createStructField("url", DataTypes.StringType, false)
        });

        Dataset<Row> df = spark.read().format("csv")
                .option("header", "true")
                .option("multiline", true)
                .option("sep", ";")
                .option("quote", "^")
                .option("dateFormat", "M/d/y")
                //.option("inferSchema", true)
                .schema(schema)
                .load("src/main/resources/amazonProducts.txt");

        df.show(5, 15);
        df.printSchema();
    }
}
