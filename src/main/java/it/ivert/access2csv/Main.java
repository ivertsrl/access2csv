package it.ivert.access2csv;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Main {

    public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
        Connection conn = null;
        try {
            Class.forName("net.ucanaccess.jdbc.UcanaccessDriver"); /* often not required for Java 6 and later (JDBC 4.x) */
            conn = DriverManager.getConnection(String.format("jdbc:ucanaccess://%s;keepMirror=mirrordb;showSchema=true", args[0]));

            DatabaseMetaData databaseMetaData = conn.getMetaData();

            try (ResultSet resultSet = databaseMetaData.getTables(null, null, null, new String[]{"TABLE"})) {
                while (resultSet.next()) {
                    String tableName = resultSet.getString("TABLE_NAME");

                    FileWriter out = new FileWriter(tableName + ".csv");

                    CSVFormat csvFormat = CSVFormat.DEFAULT.builder().build();

                    Statement st = conn.createStatement();
                    ResultSet rs = st.executeQuery("SELECT * FROM " + tableName);

                    try (final CSVPrinter printer = new CSVPrinter(out, csvFormat)) {
                        printer.printRecords(rs, true);
                    }
                }
            }
        } finally {
            if (conn != null)
                conn.close();
        }
    }
}
