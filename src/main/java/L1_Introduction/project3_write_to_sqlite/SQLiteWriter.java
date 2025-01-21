package L1_Introduction.project3_write_to_sqlite;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class SQLiteWriter {

    private static SQLiteWriter instance; // The single instance of SQLiteWriter
    private final Connection connection;

    // Private constructor to prevent direct instantiation
    private SQLiteWriter(String dbFilePath) throws SQLException {
        String url = "jdbc:sqlite:" + dbFilePath;
        this.connection = DriverManager.getConnection(url);
        this.createTable();
    }

    // Public static method to get the single instance
    public static synchronized SQLiteWriter getInstance(String dbFilePath) throws SQLException {
        if (instance == null) {
            instance = new SQLiteWriter(dbFilePath);
        }
        return instance;
    }

    // Method to create a table (called only in constructor)
    private void createTable() throws SQLException {
        String main_table = "CREATE TABLE IF NOT EXISTS employees (\n" +
                "ID INTEGER PRIMARY KEY,\n" +
                "Name TEXT NOT NULL,\n" +
                "Age INTEGER NOT NULL,\n" +
                "Department TEXT NOT NULL,\n" +
                "Salary INTEGER NOT NULL,\n" +
                "JoiningDate DATE NOT NULL,\n" +
                "LastName TEXT NOT NULL\n" +
                ");";

        try (PreparedStatement prep_statement = this.connection.prepareStatement(main_table)) {
            prep_statement.executeUpdate();
            System.out.println("Table created successfully (if not already exists).");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    // Optional method to get the connection (if needed elsewhere)
    public Connection getConnection() {
        return this.connection;
    }
}
