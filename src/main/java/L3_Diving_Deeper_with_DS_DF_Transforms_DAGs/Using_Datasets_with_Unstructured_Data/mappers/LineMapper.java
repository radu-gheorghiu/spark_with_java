package L3_Diving_Deeper_with_DS_DF_Transforms_DAGs.Using_Datasets_with_Unstructured_Data.mappers;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;

import java.util.Arrays;
import java.util.Iterator;

public class LineMapper implements FlatMapFunction<Row, String> {

    private static final long serialVersionID = 1L;

    @Override
    public Iterator<String> call(Row t) throws Exception {
        return Arrays.asList(t.toString().split(" ")).iterator();
    }
}
