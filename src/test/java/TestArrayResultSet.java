import org.junit.Test;
import raw.jdbc.ArrayResultSet;
import raw.jdbc.RawRsMetaData;

import java.lang.reflect.Type;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;


public class TestArrayResultSet {
    @Test
    public void testNulls() throws SQLException {
        // should be able to guess the right types even with the nulls
        Object[][] data = new Object[][]{
                {null, null, null},
                {1, null, null},
                {null, "1", 10000001L}
        };
        String[] columns = new String[]{"int", "string", "long"};
        int[] types = new int[]{Types.VARCHAR};
        ArrayResultSet rs = new ArrayResultSet(data, columns);
        assert( rs.next());
        assert(rs.getString(3) == null);
        rs.wasNull();
        RawRsMetaData md = (RawRsMetaData) rs.getMetaData();
        for(int n = 0 ; n < columns.length ; ++n) {
            assert(md.getColumnName(n+1).equals(columns[n]));
        }
        assert(md.getColumnCount() == 3);
        assert(md.getColumnType(1) == Types.INTEGER);
        assert(md.getColumnType(2) == Types.VARCHAR);
        assert(md.getColumnType(3) == Types.BIGINT);
    }

    @Test
    public void testEmptyArray() throws SQLException {
        Object[][] data = new Object[][]{};
        String[] columns = new String[]{"string_column"};
        int[] types = new int[]{Types.VARCHAR};
        ArrayResultSet rs = new ArrayResultSet(data, columns, types);
        assert(! rs.next());
        ResultSetMetaData md = rs.getMetaData();
        assert(md.getColumnType(1) == types[0]);
        assert(md.getColumnCount() == 1);
        assert(md.getColumnName(1).equals("string_column"));
    }
}
