import org.junit.Test;

import java.sql.*;

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
}
