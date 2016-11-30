import org.junit.Test;
import raw.jdbc.RawDriver;
import raw.jdbc.RawResultSet;
import raw.jdbc.RawStatement;

import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

public class TestConnection extends RawTest {

    @Test
    public void testGetIntCollection() throws SQLException {
        String url = "jdbc:raw:http://localhost:54321";
        RawDriver driver = new RawDriver();

        // Will get all other settings from the property file
        Connection conn = driver.connect(url, conf);
        Statement stmt = conn.createStatement();

        Integer[] numbers = {1, 2, 3, 4};
        ResultSet rs = stmt.executeQuery(objToString(numbers));
        assert (rs.getString(0).equals(numbers[0].toString()));
        try {
            rs.getInt(1);
            throw new RuntimeException("Getting from nonexistent column number must fail");

        } catch (IndexOutOfBoundsException e) {
            logger.fine("This throws as it should, all good");
        }

        int n = 0;
        do {
            assert (rs.getInt(0) == numbers[n]);
            n += 1;
        } while (rs.next());

        assert (n == numbers.length);

        Integer[][] table = {
                {1, 2, 3, 4},
                {4, 5, 6, 7},
                {8, 9, 10, 11},
        };

        rs = stmt.executeQuery(objToString(table));
        for (int i = 0; i < table.length; i++) {
            for (int j = 0; j < table[i].length; j++) {
                assert (rs.getInt(j) == table[i][j]);
            }
            rs.next();
        }
    }

    @Test
    public void testGenerateArray() {
        Integer[][] numbers = {{1, 2, 3, 4}};
        String query = objToString(numbers);

        logger.fine("int array '" + query + "'");
        assert (query.equals("collection(collection(1,2,3,4))"));

        Map<String, Object> map = new LinkedHashMap<String, Object>();
        map.put("string", "hello");
        map.put("number", 1);

        query = objToString(map);
        assert (query.equals("(string:\"hello\",number:1)"));
    }

    private String objToString(Object inobj) {
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
                sb.append(objToString(entries[0]));
            }
            for (int n = 1; n < entries.length; n++) {
                sb.append(",").append(objToString(entries[n]));
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
                sb.append(objToString(map.get(k)));
            }
            while (iter.hasNext()) {
                String k = iter.next();
                sb.append(",").append(k).append(":");
                sb.append(objToString(map.get(k)));
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
