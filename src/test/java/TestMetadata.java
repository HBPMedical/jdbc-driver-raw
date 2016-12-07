import org.junit.Test;
import raw.jdbc.RawDatabaseMetaData;
import raw.jdbc.RawResultSet;
import raw.jdbc.RsMetaData;

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
        Map<String, Object>[] records = new Map[]{
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
        RsMetaData md = (RsMetaData) rs.getMetaData();
        assert(md.getColumnCount() == 6);

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
    }

    @Test
    public void testCollection() throws SQLException {
        ResultSet rs = statement.executeQuery("collection(1, 2, 3, 4)");
        RsMetaData md = (RsMetaData) rs.getMetaData();
        assert (md.getColumnType(1) == Types.INTEGER);
        assert (md.getColumnName(1).equals(RawResultSet.SINGLE_ELEM_LABEL));
    }

    @Test
    public void testSchemas() throws SQLException {
        RawDatabaseMetaData md = (RawDatabaseMetaData) conn.getMetaData();

        ResultSet rs = md.getSchemas();
        while(rs.next()) {
            logger.fine("Schema: " + rs.getString(1) + " catalog: " + rs.getString(2));
        }
    }
}
