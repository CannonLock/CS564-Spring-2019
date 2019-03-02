
// import com.sun.org.apache.regexp.internal.RE;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class pgtest {
    private static Connection conn;
    private static Scanner in = new Scanner(System.in);

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        // String url =
        // "jdbc:postgresql://stampy.cs.wisc.edu/cs564instr?sslfactory=org.postgresql.ssl.NonValidatingFactory&ssl";
        String url = "jdbc:postgresql://localhost:5432/ShawnZhong";
        conn = DriverManager.getConnection(url);
        loop();
        conn.close();
    }

    private static void loop() throws SQLException {
        int n, seed = 0;
        Set<Integer> selected = new HashSet<>();

        String query = promptUserQuery();
        int N = getCountFromQuery(query);
        do {
            System.out.print("How many samples do you want: ");
            n = Math.min(N, in.nextInt());
            System.out.print("Please enter the seed for sampling: ");
            seed = in.nextInt();
            Integer[] sampleRowNum = sampleNumer(N, n, seed, selected);
            N -= n;
            Collections.addAll(selected, sampleRowNum);

            printResultSet(getResultSetFromQuery(query));
            System.out.println(Arrays.toString(sampleRowNum));

            if (N <= 0) {
                System.out.println("No more samples available.");
                break;
            }
            System.out.print("Enter anything to continue sampling or quit to exit: ");
            if (in.next().toLowerCase().equals("quit"))
                break;

        } while (true);
    }

    private static String promptUserQuery() {
        System.out.print("Please specify execution mode (Enter T (Table) / Q (Query)): ");
        String line = in.nextLine().toLowerCase();
        if (line.charAt(0) == 't') {
            System.out.print("Please enter table name: ");
            return getQueryFromTableName(in.nextLine());
        } else {
            System.out.print("Please enter your query: ");
            return in.nextLine();
        }
    }

    private static Integer getCountFromQuery(String query) throws SQLException {
        if (query.endsWith(";"))
            query = query.substring(0, query.length() - 1);
        ResultSet rs = getResultSetFromQuery("select count(*) from (" + query + ") orig;");
        rs.next();
        return rs.getInt(1);
    }

    private static ResultSet getResultSetFromQuery(String query) throws SQLException {
        return conn.createStatement().executeQuery(query);
    }

    private static String getQueryFromTableName(String tableName) {
        return "select * from " + tableName + ";";
    }

    private static Integer[] sampleNumer(int R, int n, int seed, Set<Integer> selected) {
        Integer[] ret = new Integer[n];
        int m = 0, t = 0, N = R + selected.size();
        Random rng = new Random(seed);
        for (int i = 1; i <= N; i++) {
            if (selected.contains(i))
                continue;
            if (rng.nextDouble() * (R - t + 1) < n - m) {
                ret[m] = i;
                m++;
            }
            t++;
        }
        return ret;
    }

    private static void printResultSet(ResultSet rs) throws SQLException {
        printHeader(rs);
        while (rs.next())
            printRow(rs);
    }

    private static void printHeader(ResultSet rs) throws SQLException {
        int columnCount = rs.getMetaData().getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
            String columnName = rs.getMetaData().getColumnName(i);
            System.out.print(columnName + "\t");
        }
        System.out.println();
    }

    private static void printRow(ResultSet rs) throws SQLException {
        int columnCount = rs.getMetaData().getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
            String className = rs.getMetaData().getColumnClassName(i);
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
            System.out.print("\t");
        }
        System.out.println();
    }
}
