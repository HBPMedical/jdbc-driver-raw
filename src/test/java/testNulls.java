import org.junit.Test;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

public class testNulls extends QueryTypeTest {

    public testNulls() throws SQLException {
    }

    @Test
    public void testIntNulls() throws SQLException {
        Statement stmt = conn.createStatement();
        String query = "collection(none, some(1),  some(3), none)";
        ResultSet rs = stmt.executeQuery(query);
        rs.next();
        rs.next();
        assert (rs.getInt(1) == 1);
        rs.next();
        assert (rs.getInt(1) == 3);
        ResultSetMetaData md = rs.getMetaData();
        assert (md.getColumnType(1) == Types.INTEGER);
    }

    @Test
    public void testRecordNulls() throws SQLException {
        Statement stmt = conn.createStatement();
        String query = "collection((_int: none, _long: none, _string: none, _double: none)," +
                "(_int: some(1), _long: some(10000000001), _string: some(\"hello\"), _double: some(1.2))," +
                "(_int: some(1), _long: some(10000000001), _string: some(\"hello\"), _double: some(1.2)))";

        ResultSet rs = stmt.executeQuery(query);
        rs.next();
        rs.next();
        assert (rs.getInt("_int") == 1);
        assert (rs.getLong("_long") == 10000000001L);
        assert (rs.getString("_string").equals("hello"));
        assert (rs.getDouble("_double") == 1.2);

        ResultSetMetaData md = rs.getMetaData();
        assert (md.getColumnType(1) == Types.INTEGER);
        assert (md.getColumnType(2) == Types.BIGINT);
        assert (md.getColumnType(3) == Types.VARCHAR);
        assert (md.getColumnType(4) == Types.DOUBLE);
    }

    @Test
    public void testNestedNulls() throws SQLException {
        Statement stmt = conn.createStatement();
        String query = "collection((_string: none, _record: none, _array: none)," +
                "(_string: some(\"hello\"), _record: some((name: \"john\", age: 23)), _array: some(collection(1,2,3)))," +
                "(_string: some(\"world\"), _record: some((name: \"jane\", age: 24)), _array: some(collection(4,5,6))))";

        ResultSet rs = stmt.executeQuery(query);
        rs.next();
        assert (rs.getString("_string") == null);
        assert (rs.getObject("_record", LinkedHashMap.class) == null);
        assert (rs.getObject("_array", ArrayList.class) == null);

        rs.next();
        assert (rs.getString("_string").equals("hello"));
        LinkedHashMap record = rs.getObject("_record", LinkedHashMap.class);
        Map expected = toMap(new Object[][]{{"name", "john"}, {"age", 23}});
        assert (record.toString().equals(expected.toString()));
        assert (Arrays.equals(rs.getObject("_array", int[].class), new int[]{1, 2, 3}));

        rs.next();
        assert (rs.getString("_string").equals("world"));
        record = rs.getObject("_record", LinkedHashMap.class);
        expected = toMap(new Object[][]{{"name", "jane"}, {"age", 24}});
        assert (record.toString().equals(expected.toString()));
        assert (Arrays.equals(rs.getObject("_array", int[].class), new int[]{4, 5, 6}));

        ResultSetMetaData md = rs.getMetaData();
        assert (md.getColumnType(1) == Types.VARCHAR);
        assert (md.getColumnType(2) == Types.STRUCT);
        assert (md.getColumnType(3) == Types.ARRAY);
    }
}
