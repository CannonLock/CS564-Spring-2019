
// import com.sun.org.apache.regexp.internal.RE;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class pgtest {
    private static Connection conn;
    private static Scanner in = new Scanner(System.in);


    public static void main(String[] args) throws Exception {
        // String url =
        // "jdbc:postgresql://stampy.cs.wisc.edu/cs564instr?sslfactory=org.postgresql.ssl.NonValidatingFactory&ssl";
        String url = "jdbc:postgresql://localhost:5432/ShawnZhong";
        conn = DriverManager.getConnection(url);
        loop();
        conn.close();
    }

    private static void loop() throws SQLException, NumberFormatException, InputMismatchException {
        int n, seed = 0;
        Set<Integer> selected = new HashSet<>();

        String query = promptUserQuery();
        int N = getCountFromQuery(query);
        System.out.print("How many samples do you want: ");
        while (true) {
            String sampleInput = in.next().toLowerCase();
            if (sampleInput.equals("quit"))
                break;

            n = Math.min(N, Integer.parseInt(sampleInput));
            if (n < 1) {
                System.out.print("Sample size input must be greater than zero, please re-enter your input: ");
                continue;
            }

            System.out.print("Please enter the seed for sampling: ");
            seed = in.nextInt();

            System.out.print("Please select output mode: (Enter table name(save in new table) / stdout(standard output))");
            String newtName = in.next();

            Integer[] sampleRowNum = sampleNumer(N, n, seed, selected);
            N -= n;
            Collections.addAll(selected, sampleRowNum);

            String rowQuery = QueryBuilder.selectRow(query, sampleRowNum);
            if (newtName.equals("stdout")) {
                ResultPrinter.print(getResultSetFromQuery(rowQuery));
            } else {
                rowQuery = QueryBuilder.saveResult(newtName, rowQuery);
                getResultSetFromQuery(rowQuery);
                System.out.println("Results are saved into table: " + newtName);
            }

            if (N <= 0) {
                System.out.println("No more samples available.");
                break;
            }
            System.out.print("Enter the numbers of the additional samples to continue or quit to exit: ");
        }
    }

    private static String promptUserQuery() {
        System.out.print("Please specify execution mode (Enter T (Table) / Q (Query)): ");
        String line = in.nextLine().toLowerCase();
        if (line.charAt(0) == 't') {
            System.out.print("Please enter table name: ");
            return QueryBuilder.selectAllFromTable(in.nextLine());
        } else if (line.charAt(0) == 'q') {
            System.out.print("Please enter your query: ");
            return in.nextLine();
        } else {
            System.out.print("Please re-enter a valid input!");
            return promptUserQuery();
        }
    }

    private static Integer getCountFromQuery(String query) throws SQLException {
        ResultSet rs = getResultSetFromQuery(QueryBuilder.getCount(query));
        rs.next();
        return rs.getInt(1);
    }

    private static ResultSet getResultSetFromQuery(String query) throws SQLException {
        System.out.println(query);
        return conn.createStatement().executeQuery(query);
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
}

class QueryBuilder {
    static String selectAllFromTable(String tableName) {
        return "select * from " + tableName + ";";
    }

    static String saveResult(String newtName, String query) {
        return "insert into " + newtName + " " + query;
    }

    static String selectRow(String query, Integer[] sampleRowNum) {
        query = truncateSemicolon(query);
        String whereClause = "where rownum in (" + IntArrayToString(sampleRowNum) + ")";
        return "select * from ( select row_Number() over () as rownum, * from (" + query + ") s1 ) s2 " + whereClause + ";";
    }

    static String getCount(String query){
        query = truncateSemicolon(query);
        return "select count(*) from (" + query + ") orig;";
    }

    private static String truncateSemicolon(String query){
        if (query.endsWith(";"))
            return query.substring(0, query.length() - 1);
        return query;
    }

    private static String IntArrayToString(Integer[] array){
        String string = Arrays.toString(array);
        return string.substring(1, string.length() - 1);
    }
}

class ResultPrinter {
    static void print(ResultSet rs) throws SQLException {
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
                case "java.lang.Long": {
                    System.out.print(rs.getLong(i));
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