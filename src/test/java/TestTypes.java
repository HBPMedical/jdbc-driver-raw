import org.junit.Test;
import raw.jdbc.RawDriver;
import raw.jdbc.RawResultSet;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class TestTypes extends QueryTypeTest {


    public TestTypes() throws SQLException {
    }

    @Test
    public void testIntCollection() throws SQLException {
        Statement stmt = conn.createStatement();
        RawResultSet rs = (RawResultSet) stmt.executeQuery("select * from collection(1,2,3,4) x where x > 2");

        assert (rs.isBeforeFirst());
        rs.next();
        assert (rs.getString(1).equals("3"));
        assert (rs.getInt(1) == 3);
        assert (rs.next());
        assert (rs.getInt(1) == 4);
        try {
            rs.getInt(2);
            throw new RuntimeException("Getting from nonexistent column number must fail");

        } catch (IndexOutOfBoundsException e) {
            logger.fine("This throws as it should, all good");
        }
        assert (!rs.next());
        assert (rs.isAfterLast());
    }

    @Test
    public void testIntList() throws SQLException {
        Statement stmt = conn.createStatement();

        Integer[] list = {1, 2, 3};

        ResultSet rs = stmt.executeQuery(objToQuery(list));
        for (int i = 0; i < list.length; i++) {
            rs.next();
            assert (rs.getInt(1) == list[i]);
        }
    }

    @Test
    public void testStringList() throws SQLException {
        Statement stmt = conn.createStatement();

        String[] list =  new String[] {"1", "2", "3", "4"};

        ResultSet rs = stmt.executeQuery(objToQuery(list));
        for (int i = 0; i < list.length; i++) {
            rs.next();
            assert (rs.getString(1).equals(list[i]));
        }
        assert (!rs.next());
    }

    @Test
    public void testRecord() throws SQLException {
        Statement stmt = conn.createStatement();

        Map[] records = new Map[]{
                toMap(new Object[][]{{"_string", "hello"}, {"_int", 1}, {"_long", 100000000001L}, {"_double", 0.01d}, {"_float", 1.01f}}),
                toMap(new Object[][]{{"_string", "world"}, {"_int", 2}, {"_long", 100000000002L}, {"_double", 0.01d}, {"_float", 1.01f}}),
                toMap(new Object[][]{{"_string", "again"}, {"_int", 3}, {"_long", 100000000003L}, {"_double", 0.01d}, {"_float", 1.01f}}),
        };

        String query = objToQuery(records);
        logger.fine("query: " + query);
        ResultSet rs = stmt.executeQuery(query);

        for (int n = 0; n < records.length; n++) {
            rs.next();
            assert (rs.getString("_string").equals(records[n].get("_string")));
            assert (rs.getString(1).equals(records[n].get("_string")));

            assert ((Integer) records[n].get("_int") == rs.getInt("_int"));
            assert (rs.getInt(2) == (Integer) records[n].get("_int"));

            assert (rs.getLong("_long") == (Long) records[n].get("_long"));
            assert (rs.getLong(3) == (Long) records[n].get("_long"));

            assert (rs.getDouble("_double") == (Double) records[n].get("_double"));
            assert (rs.getDouble(4) == (Double) records[n].get("_double"));

            assert (rs.getFloat("_float") == (Float) records[n].get("_float"));
            assert (rs.getFloat(5) == (Float) records[n].get("_float"));
        }

        assert (rs.getString("_string").equals("again"));
    }

    @Test
    public void testArrays() throws SQLException {
        Statement stmt = conn.createStatement();

        Map[] records = new Map[]{
                toMap(new Object[][]{
                        {"_string", new String[]{"hello", "world", "again"}},
                        {"_int", new int[]{1, 2, 3}},
                        {"_long", new Long[]{100000000001L, 100000000002L, 100000000003L}},
                        {"_double", new double[]{0.01d, 0.02d, 0.03d}},
                        {"_float", new float[]{0.01f, 0.02f, 0.03f}}
                }),
                toMap(new Object[][]{
                        {"_string", new String[]{"more", "of the", "same"}},
                        {"_int", new int[]{4, 5, 6}},
                        {"_long", new Long[]{100000000004L, 100000000005L, 100000000006L}},
                        {"_double", new double[]{0.04d, 0.05d, 0.06d}},
                        {"_float", new float[]{0.04f, 0.05f, 0.06f}}
                }),
                toMap(new Object[][]{
                        {"_string", new String[]{"repeats", "all over", "again"}},
                        {"_int", new int[]{7, 8, 9}},
                        {"_long", new Long[]{100000000007L, 100000000008L, 100000000009L}},
                        {"_double", new double[]{0.07d, 0.08d, 0.09d}},
                        {"_float", new float[]{0.07f, 0.08f, 0.09f}}
                })
        };

        String query = objToQuery(records);
        logger.fine("query: " + query);
        RawResultSet rs = (RawResultSet) stmt.executeQuery(query);

        for (int n = 0; n < records.length; n++) {
            rs.next();
            String[] strings = rs.getObject("_string", String[].class);
            assert (Arrays.equals(strings, (String[]) records[n].get("_string")));

            int[] ints = rs.getObject("_int", int[].class);
            assert (Arrays.equals(ints, (int[]) records[n].get("_int")));

            Long[] longs = rs.getObject("_long", Long[].class);
            assert (Arrays.equals(longs, (Long[]) records[n].get("_long")));

            float[] floats = rs.getObject("_float", float[].class);
            assert (Arrays.equals(floats, (float[]) records[n].get("_float")));

            double[] doubles = rs.getObject("_double", double[].class);
            assert (Arrays.equals(doubles, (double[]) records[n].get("_double")));
        }

    }

    @Test
    public void testNested() throws SQLException {
        Map[] records = new Map[]{
                toMap(new Object[][]{
                        {"_string", "hello"},
                        {"_list", new int[]{1, 2, 3}},
                        {"_record", toMap(new Object[][]{{"_string", "hello"}, {"_int", 1}, {"_long", 100000000001L}, {"_double", 0.01d}, {"_float", 1.01f}})}
                }),
                toMap(new Object[][]{
                        {"_string", "world"},
                        {"_list", new int[]{4, 5, 6}},
                        {"_record", toMap(new Object[][]{{"_string", "world"}, {"_int", 2}, {"_long", 100000000002L}, {"_double", 0.03d}, {"_float", 1.01f}})}
                }),
                toMap(new Object[][]{
                        {"_string", "world"},
                        {"_list", new int[]{7, 8, 9}},
                        {"_record", toMap(new Object[][]{{"_string", "again"}, {"_int", 3}, {"_long", 100000000003L}, {"_double", 0.04d}, {"_float", 1.01f}})}
                })
        };
        Statement stmt = conn.createStatement();
        String query = objToQuery(records);
        logger.fine("query: " + query);
        RawResultSet rs = (RawResultSet) stmt.executeQuery(query);
        for (int n = 0; n < records.length; n++) {
            rs.next();
            String string = rs.getObject("_string", String.class);
            assert (string.equals(records[n].get("_string")));

            int[] ints = rs.getObject("_list", int[].class);
            assert (Arrays.equals(ints, (int[]) records[n].get("_list")));

            LinkedHashMap obj = rs.getObject("_record", LinkedHashMap.class);
            LinkedHashMap expected = (LinkedHashMap<String, Object>) records[n].get("_record");
            logger.fine("got:" + obj + " expected: " + expected);
            //TODO: find a way of comparing LinkedHashMaps
            assert (obj.toString().equals(expected.toString()));
        }
    }

    @Test
    public void getNext() throws SQLException {
        Statement stmt = conn.createStatement();
        Integer[] values = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        ResultSet rs = stmt.executeQuery(objToQuery(values));
        // fetchSize 1 will make it contact the server on each next()
        rs.setFetchSize(1);
        for (Integer i : values) {
            rs.next();
            assert (rs.getInt(1) == i);
        }
    }

}
