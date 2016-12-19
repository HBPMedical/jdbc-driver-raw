import org.junit.Test;
import raw.jdbc.ArrayResultSet;
import raw.jdbc.RawResultSet;
import raw.jdbc.RawRsMetaData;

import java.sql.*;
import java.util.Map;

public class TestMetadata extends TestQueries {

    Statement statement;

    public TestMetadata() throws SQLException {
        super();
        statement = conn.createStatement();
    }

    @Test
    public void tesTypes() throws SQLException {
        Map[] records = new Map[]{
                toMap(new Object[][]{
                        {"_string", "hello"},
                        {"_int", 1},
                        {"_long", 100000000001L},
                        {"_double", 0.01d},
                        {"_float", 1.01f},
                        {"_array", new int[]{1, 2, 3}}
                }),
                toMap(new Object[][]{
                        {"_string", "world"},
                        {"_int", 2},
                        {"_long", 100000000002L},
                        {"_double", 0.01d},
                        {"_float", 1.01f},
                        {"_array", new int[]{4, 5, 6}}
                })

        };
        ResultSet rs = statement.executeQuery(objToQuery(records));
        RawRsMetaData md = (RawRsMetaData) rs.getMetaData();
        assert (md.getColumnCount() == 6);
        logger.fine("col 1: type: " + md.getColumnType(1));
        logger.fine("col 6: type: " + md.getColumnType(6));

        assert (md.getColumnType(1) == Types.VARCHAR);
        assert (md.getColumnType(2) == Types.INTEGER);
        assert (md.getColumnType(3) == Types.BIGINT);
        assert (md.getColumnType(4) == Types.DOUBLE);
        assert (md.getColumnType(5) == Types.DOUBLE);
        assert (md.getColumnType(6) == Types.ARRAY);

        assert (md.getColumnName(1).equals("_string"));
        assert (md.getColumnName(2).equals("_int"));
        assert (md.getColumnName(3).equals("_long"));
        assert (md.getColumnName(4).equals("_double"));
        assert (md.getColumnName(5).equals("_float"));
        assert (md.getColumnName(6).equals("_array"));

        assert (md.getColumnLabel(1).equals("_string"));
        assert (md.getColumnLabel(2).equals("_int"));
        assert (md.getColumnLabel(3).equals("_long"));
        assert (md.getColumnLabel(4).equals("_double"));
        assert (md.getColumnLabel(5).equals("_float"));
        assert (md.getColumnLabel(6).equals("_array"));
    }

    @Test
    public void testCollection() throws SQLException {
        ResultSet rs = statement.executeQuery("collection(1, 2, 3, 4)");
        RawRsMetaData md = (RawRsMetaData) rs.getMetaData();
        assert (md.getColumnCount() == 1);
        assert (md.getColumnType(1) == Types.INTEGER);
        assert (md.getColumnName(1).equals(RawResultSet.SINGLE_ELEM_LABEL));
    }

    @Test
    public void testSchemas() throws SQLException {
        DatabaseMetaData md = conn.getMetaData();

        ResultSet rs = md.getSchemas();
        while (rs.next()) {
            logger.fine("Schema: " + rs.getString(1) + " catalog: " + rs.getString(2));
        }
    }

    @Test
    public void testArrayResultSet() throws SQLException {
        Object[][] data = new Object[][]{
                {"hello", 1, 1.1, 1000001L},
                {"world", 2, 1.2, 1000002L},
                {"again", 3, 1.3, 1000003L},
                {"more", 4, 1.4, 1000004L}
        };

        String[] fields = new String[]{"_string", "_int", "_double", "_long"};
        ResultSet rs = new ArrayResultSet(data, fields);

        for (int i = 0; i < data.length; i++) {
            rs.next();
            for (int j = 0; j < data[i].length; j++) {
                assert (rs.getObject(j + 1).equals(data[i][j]));
            }

            assert (rs.getString(1).equals(data[i][0]));
            assert (rs.getInt(2) == (Integer) data[i][1]);
            assert (rs.getDouble(3) == (Double) data[i][2]);
            assert (rs.getLong(4) == (Long) data[i][3]);
        }

        ResultSetMetaData md = rs.getMetaData();
        int[] types = new int[]{Types.VARCHAR, Types.INTEGER, Types.DOUBLE, Types.BIGINT};
        for (int i = 0; i < md.getColumnCount(); i++) {
            assert (md.getColumnType(i + 1) == types[i]);
            assert (md.getColumnName(i + 1).equals(fields[i]));
        }
    }

    @Test
    public void getTableMetadata() throws SQLException {
        DatabaseMetaData md = conn.getMetaData();

        ResultSet rs = md.getTables(null, null, "", null);
        //TODO: register test files and get the table metadata
        while (rs.next()) {
            logger.fine("table " + rs.getString(3));
        }
    }
}
