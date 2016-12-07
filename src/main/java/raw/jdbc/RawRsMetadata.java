package raw.jdbc;


import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;

public class RawRsMetadata extends RsMetaData {

    public RawRsMetadata(Object[] data) throws SQLException {
        super(null, null);
        if (data.length > 0) {
            Object obj = data[0];
            if (obj.getClass() == LinkedHashMap.class) {
                Map<String, Object> map = (Map) obj;
                columnNames = map.keySet().toArray(new String[]{});
                types = new int[columnNames.length];
                for (int i = 0; i < columnNames.length; i++) {
                    types[i] = objToType(map.get(columnNames[i]));
                }
            } else {
                columnNames = new String[]{RawResultSet.SINGLE_ELEM_LABEL};
                types = new int[]{objToType(obj)};
            }
        } else {
            throw new SQLException("cannot get metadata for empty array");
        }
    }

}