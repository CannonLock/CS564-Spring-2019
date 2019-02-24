import com.sun.org.apache.regexp.internal.RE;

import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class pgtest {
    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        String url = "jdbc:postgresql://stampy.cs.wisc.edu/cs564instr?sslfactory=org.postgresql.ssl.NonValidatingFactory&ssl";
        Connection conn = DriverManager.getConnection(url);
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("select * from hw2.sales limit 100");
        while (rs.next()) {
            printRow(rs);
        }
        // close up shop
        rs.close();
        st.close();
        conn.close();
    }

    private static void printRow(ResultSet rs) throws SQLException {
        int columnCount = rs.getMetaData().getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
            String className = rs.getMetaData().getColumnClassName(i);
            System.out.print(" ");
            switch (className) {
                case "java.lang.Integer": {
                    System.out.print(rs.getInt(i));
                    break;
                }
                case "java.lang.Float": {
                    System.out.print(rs.getFloat(i));
                    break;
                }
                case "java.sql.Date": {
                    System.out.print(rs.getDate(i));
                    break;
                }
                default:
                    throw new IllegalStateException("Unexpected value: " + className);
            }
        }
        System.out.println(" ");
    }
}
