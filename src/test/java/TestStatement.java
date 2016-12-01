import org.junit.Test;
import raw.jdbc.RawDriver;

import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

public class TestStatement extends RawTest {


    Connection conn;

    public TestStatement() throws SQLException {
        String url = "jdbc:raw:http://localhost:54321";
        RawDriver driver = new RawDriver();
        conn = driver.connect(url, conf);
    }

    @Test
    public void testInCollection() throws SQLException {
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("select * from collection(1,2,3,4) x where x > 2");
        assert (rs.getString(0).equals("3"));
        assert (rs.getInt(0) == 3);
        assert (rs.next());
        assert (rs.getInt(0) == 4);
        try {
            rs.getInt(1);
            throw new RuntimeException("Getting from nonexistent column number must fail");

        } catch (IndexOutOfBoundsException e) {
            logger.fine("This throws as it should, all good");
        }
        assert (!rs.next());
    }

    @Test
    public void testIntTable() throws SQLException {
        Statement stmt = conn.createStatement();

        Integer[][] table = {
                {1, 2, 3, 4},
                {4, 5, 6, 7},
                {8, 9, 10, 11},
        };

        ResultSet rs = stmt.executeQuery(objToQuery(table));
        for (int i = 0; i < table.length; i++) {
            for (int j = 0; j < table[i].length; j++) {
                assert (rs.getInt(j) == table[i][j]);
            }
            rs.next();
        }
    }

    @Test
    public void testStringTable() throws SQLException {
        Statement stmt = conn.createStatement();

        String[][] table = {
                {"1", "2", "3", "4"},
                {"4", "5", "6", "7"},
                {"8", "9", "10", "11"},
        };

        ResultSet rs = stmt.executeQuery(objToQuery(table));
        for (int i = 0; i < table.length; i++) {
            for (int j = 0; j < table[i].length; j++) {
                assert (rs.getString(j).equals(table[i][j]));
            }
            rs.next();
        }
    }

    @Test
    public void testRecord() throws SQLException {
        Statement stmt = conn.createStatement();

        Map<String, Object>[] records = new Map[]{
                toMap(new Object[][]{{"_string", "hello"}, {"_int", 1},{"_long", 100000000001L}}),
                toMap(new Object[][]{{"_string", "world"}, {"_int", 2},{"_long", 100000000002L}}),
                toMap(new Object[][]{{"_string", "again"}, {"_int", 3},{"_long", 100000000003L}}),
        };

        String query = objToQuery(records);
        logger.fine("query: " + query);
        ResultSet rs = stmt.executeQuery(query);
        logger.fine("_string: " + rs.getString("_string"));
        assert (rs.getString("_string").equals("hello"));
    }

    private Map<String, Object> toMap(Object[][] entries) {
        Map<String, Object> map = new LinkedHashMap();
        for (Object[] e : entries) {
            map.put((String) e[0], e[1]);
        }
        return map;
    }

    private String objToQuery(Object inobj) {
        Object obj = convertTypes(inobj);
        if (obj.getClass() == String.class) {
            return "\"" + obj + "\"";
        } else if (obj.getClass() == Integer.class ||
                obj.getClass() == Long.class ||
                obj.getClass() == Double.class) {
            return "" + obj + "";
        } else if (obj.getClass().isArray()) {
            StringBuilder sb = new StringBuilder("collection(");
            Object[] entries;
            if (obj.getClass() == int.class) {
                entries = (Integer[]) obj;
            } else {
                entries = (Object[]) obj;
            }

            if (entries.length > 0) {
                sb.append(objToQuery(entries[0]));
            }
            for (int n = 1; n < entries.length; n++) {
                sb.append(",").append(objToQuery(entries[n]));
            }
            sb.append(")");
            return sb.toString();
        } else if (obj.getClass() == LinkedHashMap.class) {
            LinkedHashMap<String, Object> map = (LinkedHashMap<String, Object>) obj;
            Set<String> keys = map.keySet();

            StringBuilder sb = new StringBuilder("(");
            Iterator<String> iter = keys.iterator();
            if (iter.hasNext()) {
                String k = iter.next();
                sb.append(k).append(":");
                sb.append(objToQuery(map.get(k)));
            }
            while (iter.hasNext()) {
                String k = iter.next();
                sb.append(",").append(k).append(":");
                sb.append(objToQuery(map.get(k)));
            }
            sb.append(")");
            return sb.toString();
        } else {
            throw new RuntimeException("Unsupported type " + obj.getClass());
        }
    }

    private Object convertTypes(Object obj) {
        if (obj.getClass() == int.class) {
            return (Integer) obj;
        } else if (obj.getClass() == double.class) {
            return (Double) obj;
        } else {
            return obj;
        }
    }
}
