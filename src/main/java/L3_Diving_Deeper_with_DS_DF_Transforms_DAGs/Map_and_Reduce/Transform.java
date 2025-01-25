package L3_Diving_Deeper_with_DS_DF_Transforms_DAGs.Map_and_Reduce;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class Transform {

    public void start(){
        SparkSession spark = new SparkSession.Builder()
                .appName("Transform")
                .master("local")
                .getOrCreate();

        String [] stringList = new String[] {"Banana", "Apple", "Pear", "Drum", "Bear"};

        List<String> data = Arrays.asList(stringList);
        Dataset<String> ds = spark.createDataset(data, Encoders.STRING());

        // here are 2 examples on how we would use the "map" pattern
        // method 1 - using the map function
        Dataset<String> ds1;
        ds1 = ds.map(new StringMapper(), Encoders.STRING());
        ds1.show();

        // method 2 - alternative using lambda functions
        Dataset<String> ds2;
        ds2 = ds.map((MapFunction<String, String>) row -> "lambda_word: " + row, Encoders.STRING());
        ds2.show();

        // now we're going to test out the "reduce" pattern
        String reduced_text = ds.reduce(new StringReducer());
        System.out.println(reduced_text);
    }

    static class StringMapper implements MapFunction<String, String>, Serializable {

        private static final long serialVersionUID = 1L;

        @Override
        public String call(String value) throws Exception {
            return "word: " + value;
        }
    }

    static class StringReducer implements ReduceFunction<String>, Serializable {

        private static final long serialVersionUID = 1L;

        @Override
        public String call(String v1, String v2) throws Exception {
            return v1 + "," + v2;
        }
    }

}
