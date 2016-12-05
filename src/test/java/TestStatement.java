import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;
import raw.jdbc.RawDriver;
import raw.jdbc.RawResultSet;
import raw.jdbc.RawStatement;

import java.lang.reflect.Array;
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
        RawResultSet rs = (RawResultSet) stmt.executeQuery("select * from collection(1,2,3,4) x where x > 2");

        assert (rs.isBeforeFirst());
        rs.next();
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
        assert (rs.isAfterLast());
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
            rs.next();
            for (int j = 0; j < table[i].length; j++) {
                assert (rs.getInt(j) == table[i][j]);
            }
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
            rs.next();
            for (int j = 0; j < table[i].length; j++) {
                assert (rs.getString(j).equals(table[i][j]));
            }
        }
    }

    @Test
    public void testRecord() throws SQLException {
        Statement stmt = conn.createStatement();

        Map<String, Object>[] records = new Map[]{
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
            assert (rs.getString(0).equals(records[n].get("_string")));

            assert ((Integer) records[n].get("_int") == rs.getInt("_int"));
            assert (rs.getInt(1) == (Integer) records[n].get("_int"));

            assert (rs.getLong("_long") == (Long) records[n].get("_long"));
            assert (rs.getLong(2) == (Long) records[n].get("_long"));

            assert (rs.getDouble("_double") == (Double) records[n].get("_double"));
            assert (rs.getDouble(3) == (Double) records[n].get("_double"));

            assert (rs.getFloat("_float") == (Float) records[n].get("_float"));
            assert (rs.getFloat(4) == (Float) records[n].get("_float"));
        }

        assert (rs.getString("_string").equals("again"));
    }

    @Test
    public void testArrays() throws SQLException {
        Statement stmt = conn.createStatement();

        Map<String, Object>[] records = new Map[]{
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
        Map<String, Object>[] records = new Map[]{
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

            LinkedHashMap<String, Object> obj = rs.getObject("_record", LinkedHashMap.class);
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
            assert (rs.getInt(0) == i);
        }
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
                obj.getClass() == Double.class ||
                obj.getClass() == Float.class) {
            return "" + obj + "";
        } else if (obj.getClass().isArray()) {
            StringBuilder sb = new StringBuilder("collection(");
            Object[] entries;
            if (obj.getClass() == int[].class) {
                int[] l = (int[]) obj;
                entries = new Integer[l.length];
                for (int i = 0; i < l.length; i++) {
                    entries[i] = l[i];
                }
            } else if (obj.getClass() == float[].class) {
                float[] l = (float[]) obj;
                entries = new Float[l.length];
                for (int i = 0; i < l.length; i++) {
                    entries[i] = l[i];
                }
            } else if (obj.getClass() == double[].class) {
                double[] l = (double[]) obj;
                entries = new Double[l.length];
                for (int i = 0; i < l.length; i++) {
                    entries[i] = l[i];
                }
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
        } else if (obj.getClass() == float.class) {
            return (Float) obj;
        } else {
            return obj;
        }
    }
}
