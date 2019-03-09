import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class pgtest {

    private static List<String> tables;

    public static void main(String[] args) throws SQLException {
        String url =
                "jdbc:postgresql://stampy.cs.wisc.edu/cs564instr?sslfactory=org.postgresql.ssl.NonValidatingFactory&ssl";
//        String url = "jdbc:postgresql://localhost:5432/ShawnZhong";
        SQLExecutor.connect(url);
        tables = new ArrayList<>();
        while (true) {
            try {
                loop();
            } catch (SQLException e) {
                System.out.println("Invalid Query");
            }
        }
    }

    private static void loop() throws SQLException {
        Set<Integer> selected = new HashSet<>();
        String query = Prompter.userQuery();
        if (query == null)
            System.exit(0);
        int N = SQLExecutor.getCount(query);

        while (true) {
            int n = Math.min(N, Prompter.sampleSize());
            long seed = Prompter.seed();
            String tableName = Prompter.tableName();
            tables.add(tableName);

            Integer[] sampleRowNum = sampleNumer(N, n, seed, selected);
            N -= n;
            Collections.addAll(selected, sampleRowNum);

            SQLExecutor.executeUpdate(QueryBuilder.dropTable(tableName));
            SQLExecutor.executeUpdate(QueryBuilder.sampleRowIntoTable(query, sampleRowNum, tableName));
            SQLExecutor.executeUpdate(QueryBuilder.dropRowNum(tableName));

            if (tableName == null) {
                ResultPrinter.print(SQLExecutor.executeQuery(QueryBuilder.selectAllFromTable(tableName)));
            } else
                System.out.println("Results are saved into table: " + tableName);

            if (N <= 0) {
                System.out.println("No more samples available.");
                break;
            }
            if (!Prompter.continueSample())
                break;
        }
    }

    private static Integer[] sampleNumer(int R, int n, long seed, Set<Integer> selected) {
        Integer[] ret = new Integer[n];
        int m = 0, t = 0, N = R + selected.size();
        Random rng = new Random(seed);
        for (int i = 1; i <= N; i++) {
            if (selected.contains(i))
                continue;
            if (rng.nextDouble() * (R - t) < n - m) {
                ret[m] = i;
                m++;
            }
            t++;
        }
        return ret;
    }
}

class Prompter {
    private static Scanner in = new Scanner(System.in);
    private static long seed = 0;

    static boolean continueSample() {
        try {
            String line = prompt("Do you want continue sampling? (Enter Y/N): ");
            if (line.charAt(0) == 'y') {
                return true;
            } else if (line.charAt(0) == 'n') {
                return false;
            } else {
                throw new IllegalArgumentException();
            }
        } catch (Exception e) {
            System.out.println("Input invalid.");
            return continueSample();
        }
    }

    static String tableName() {
        try {
            String line = prompt("Please select output mode (Enter S (Save in new table) / O (Outout)): ");
            if (line.charAt(0) == 's') {
                String tableName = prompt("Please enter table name: ");
                if (tableName.length() == 0)
                    throw new IllegalArgumentException();

                return tableName;
            } else if (line.charAt(0) == 'o') {
                return null;
            } else {
                throw new IllegalArgumentException();
            }
        } catch (Exception e) {
            System.out.println("Seed must be a long integer. Please re-enter your input");
            return tableName();
        }
    }

    static String userQuery() {
        try {
            String line = prompt("Please specify execution mode (Enter T (Table) / Q (Query) / E (Exit)): ");
            if (line.charAt(0) == 't') {
                String tableName = prompt("Please enter table name: ");
                if (tableName.length() == 0)
                    throw new IllegalArgumentException();
                return QueryBuilder.selectAllFromTable(tableName);
            } else if (line.charAt(0) == 'q') {
                return prompt("Please enter your query: ");
            } else if (line.charAt(0) == 'e') {
                return null;
            } else {
                throw new IllegalArgumentException();
            }
        } catch (Exception e) {
            System.out.println("Input invalid.");
            return userQuery();
        }
    }

    static int sampleSize() {
        try {
            String line = prompt("How many samples do you want: ");
            int sampleSize = Integer.parseInt(line);
            if (sampleSize <= 0)
                throw new IllegalArgumentException();
            return sampleSize;
        } catch (Exception e) {
            System.out.println("Sample size input must be greater than zero. Please re-enter your input");
            return sampleSize();
        }
    }

    static long seed() {
        String line = prompt("Do you want to set seed? (Enter Y/N): ");
        if (line.length() == 0 || line.charAt(0) != 'y') {
            System.out.println("Use previous seed: " + seed);
            return seed;
        }

        while (true) {
            try {
                return seed = Long.parseLong(prompt("Please enter the seed for sampling: "));
            } catch (Exception e) {
                System.out.println("Seed must be a long integer. Please re-enter your input");
            }
        }
    }

    private static String prompt(String message) {
        System.out.print(message);
        return in.nextLine().toLowerCase();
    }
}

class SQLExecutor {
    private static Connection conn;

    static void connect(String url) {
        try {
            conn = DriverManager.getConnection(url);
            executeUpdate(QueryBuilder.setPath());
        } catch (SQLException e) {
            System.out.println("Cannot connect to " + url);
            System.exit(1);
        }
    }

    static Integer getCount(String query) throws SQLException {
        ResultSet rs = executeQuery(QueryBuilder.getCount(query));
        rs.next();
        return rs.getInt(1);
    }

    static ResultSet executeQuery(String query) throws SQLException {
//        System.out.println(query);
        return conn.createStatement().executeQuery(query);
    }

    static int executeUpdate(String query) throws SQLException {
//        System.out.println(query);
        return conn.createStatement().executeUpdate(query);
    }
}

class QueryBuilder {
    private static final String TMP_TABLE_NAME = "panujxvbft";

    static String setPath() {
        return "set search_path to public, hw2;";
    }

    static String dropRowNum(String tableName) {
        return "alter table " + preprocessTableName(tableName) + " drop column rownum;";
    }

    static String dropTable(String tableName) {
        return "drop table if exists " + preprocessTableName(tableName) + ";";
    }

    static String selectAllFromTable(String tableName) {
        return "select * from " + preprocessTableName(tableName) + ";";
    }

    static String sampleRowIntoTable(String query, Integer[] sampleRowNum, String tableName) {
        return "select * into " + preprocessTableName(tableName) + " from ( select row_Number() over () as rownum, * from ("
                + truncateSemicolon(query) + ") s1 ) s2 where s2.rownum in (" + IntArrayToString(sampleRowNum)
                + ");";
    }

    static String getCount(String query) {
        query = truncateSemicolon(query);
        return "select count(*) from (" + query + ") orig;";
    }

    private static String preprocessTableName(String tableName) {
        if (tableName == null)
            tableName = TMP_TABLE_NAME;
        return tableName;
    }

    private static String truncateSemicolon(String query) {
        query = query.trim();
        if (query.endsWith(";"))
            return query.substring(0, query.length() - 1);
        return query;
    }

    private static String IntArrayToString(Integer[] array) {
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
            if ("java.lang.Integer".equals(className)) {
                System.out.print(rs.getInt(i));
            } else if ("java.lang.Long".equals(className)) {
                System.out.print(rs.getLong(i));
            } else if ("java.lang.Float".equals(className)) {
                System.out.print(rs.getFloat(i));
            } else if ("java.sql.Date".equals(className)) {
                System.out.print(rs.getDate(i));
            } else {
                throw new IllegalStateException("Unexpected value: " + className);
            }
            System.out.print("\t");
        }
        System.out.println();
    }
}