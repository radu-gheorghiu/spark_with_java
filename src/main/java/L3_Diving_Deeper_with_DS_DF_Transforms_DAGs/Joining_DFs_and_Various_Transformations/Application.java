package L3_Diving_Deeper_with_DS_DF_Transforms_DAGs.Joining_DFs_and_Various_Transformations;

import org.apache.spark.sql.*;
import org.apache.spark.sql.SparkSession;

public class Application {
    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder().appName("Joining Dataframes").master("local").getOrCreate();

        String studentsFile = "src/main/resources/L2/students.csv";

        Dataset<Row> studentsDf = spark.read().format("csv")
                .option("inferSchema", true)
                .option("header", true)
                .load(studentsFile);

        String gradeChartFile = "src/main/resources/L2/grade_chart.csv";

        Dataset<Row> gradesDf = spark.read().format("csv")
                .option("inferSchema", true)
                .option("header", true)
                .load(gradeChartFile);

        Dataset<Row> studentDetails = studentsDf.join(gradesDf, studentsDf.col("GPA").equalTo(gradesDf.col("GPA")))
                .filter(studentsDf.col("GPA").between(2, 4))
                .select(
                        studentsDf.col("student_id"),
                        studentsDf.col("student_name"),
                        studentsDf.col("State"),
                        studentsDf.col("GPA"),
                        studentsDf.col("favorite_book_title"),
                        studentsDf.col("working"),
                        gradesDf.col("letter_grade")
                );
        studentDetails.show(5);

    }
}
