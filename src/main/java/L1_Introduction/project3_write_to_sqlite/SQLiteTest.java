package L1_Introduction.project3_write_to_sqlite;

import java.sql.Connection;
import java.sql.DriverManager;

public class SQLiteTest {
    public static void main(String[] args) {
        String url = "jdbc:sqlite:C:\\Code\\GitRepos\\tutorial_spark_with_java\\data\\sparkwriterapp.db";
        try {
            // Load the SQLite JDBC driver
            Class.forName("org.sqlite.JDBC");

            // Establish connection
            Connection conn = DriverManager.getConnection(url);
            if (conn != null) {
                System.out.println("Connection to SQLite has been established.");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

