package L3_Diving_Deeper_with_DS_DF_Transforms_DAGs.Using_Datasets_with_POJOs;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;

public class Transform {

    public void start() {
        SparkSession spark = new SparkSession.Builder()
                .appName("Transform")
                .master("local")
                .getOrCreate();

        String filename = "src/main/resources/L3/houses.csv";

            Dataset<Row> df = spark.read().format("csv")
                    .option("inferSchema", true)
                    .option("header", true)
                    .option("sep", ";")
                    .load(filename);

            df.show(5);
            df.printSchema();

            System.out.println("===============================================");
            System.out.println("Converting a Dataset to a Dataframe");

            Dataset<House> houseDs = df.map(new HouseMapper(), Encoders.bean(House.class));
            Dataset<Row> houseDf = houseDs.toDF();
            houseDf.show();

    }

    public static class HouseMapper implements MapFunction<Row, House> {

        public House call(Row value) throws Exception{
            House h = new House();
            h.setId(value.getAs("id"));
            h.setAddress(value.getAs("address"));
            h.setSqft(value.getAs("sqft"));
            h.setPrice(value.getAs("price"));

            String vacancyDateString = value.getAs("vacantBy").toString();

            if (vacancyDateString != null){
                SimpleDateFormat parser = new SimpleDateFormat("yyyy-MM-dd");
                h.setVacantBy(parser.parse(vacancyDateString));
            }

            return h;
        }

    }
}