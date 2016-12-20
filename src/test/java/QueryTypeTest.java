import raw.jdbc.RawDriver;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class QueryTypeTest extends RawTest {

    Connection conn;

    public QueryTypeTest() throws SQLException {
        String url = "jdbc:raw:http://localhost:54321";
        RawDriver driver = new RawDriver();
        conn = driver.connect(url, conf);
    }

    static protected Map<String, Object> toMap(Object[][] entries) {
        Map<String, Object> map = new LinkedHashMap<String, Object>();
        for (Object[] e : entries) {
            map.put((String) e[0], e[1]);
        }
        return map;
    }

    static protected String objToQuery(Object inobj) {
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

    static protected Object convertTypes(Object obj) {
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
