package raw.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by torcato on 06.12.16.
 */
public class RawResultSetMetaData implements ResultSetMetaData {

    String[] columnNames;
    int[] types;
    Object obj;
    boolean isRecord = false;

    RawResultSetMetaData(Object[] data) throws SQLException {
        if (data.length > 0) {
            obj = data[0];
            if (obj.getClass() == LinkedHashMap.class) {
                Map<String, Object> map = (Map) obj;
                columnNames = map.keySet().toArray(new String[]{});
                types = new int[columnNames.length];
                isRecord = true;
                for (int i = 0; i < columnNames.length; i++){
                    String name = columnNames[i];
                    types[i] = objToType(map.get(name));
                }
            } {
                columnNames = new String[]{"0"};
                types = new int[]{objToType(obj)};
            }
        }
    }

    static private int objToType(Object obj) throws SQLException {
        if (obj.getClass() == Long.class) {
            return Types.INTEGER;
        } else if (obj.getClass() == String.class) {
            return Types.VARCHAR;
        } else if (obj.getClass() == Double.class) {
            return Types.DOUBLE;
        } else if (obj.getClass() == LinkedHashMap.class) {
            return Types.JAVA_OBJECT;
        } else if (obj.getClass() == ArrayList.class) {
            return Types.ARRAY;
        } else {
            throw new SQLException("Unsupported type " + obj.getClass());
        }
    }

    public int getColumnCount() throws SQLException {
        return columnNames.length;
    }

    public boolean isAutoIncrement(int column) throws SQLException {
        return false;
    }

    public boolean isCaseSensitive(int column) throws SQLException {
        return false;
    }

    public boolean isSearchable(int column) throws SQLException {
        return false;
    }

    public boolean isCurrency(int column) throws SQLException {
        return false;
    }

    public int isNullable(int column) throws SQLException {
        return 0;
    }

    public boolean isSigned(int column) throws SQLException {
        return false;
    }

    public int getColumnDisplaySize(int column) throws SQLException {
        return 0;
    }

    public String getColumnLabel(int column) throws SQLException {
        return columnNames[column];
    }

    public String getColumnName(int column) throws SQLException {
        return columnNames[column];
    }

    public String getSchemaName(int column) throws SQLException {
        return "unknown";
    }

    public int getPrecision(int column) throws SQLException {
        return 0;
    }

    public int getScale(int column) throws SQLException {
        return 0;
    }

    public String getTableName(int column) throws SQLException {
        return "unknown";
    }

    public String getCatalogName(int column) throws SQLException {
        return "unknown";
    }

    public int getColumnType(int column) throws SQLException {
        return types[column];
    }

    public String getColumnTypeName(int column) throws SQLException {
        if( isRecord){
            Map<String, Object> map = (Map) obj;
            String key = columnNames[column];
            return map.get(key).getClass().getName();
        } else if (column == 0) {
            return obj.getClass().getName();
        } else {
            return String.class.getName();
        }
    }

    public boolean isReadOnly(int column) throws SQLException {
        return true;
    }

    public boolean isWritable(int column) throws SQLException {
        return false;
    }

    public boolean isDefinitelyWritable(int column) throws SQLException {
        return false;
    }

    public String getColumnClassName(int column) throws SQLException {
        return getColumnTypeName(column);
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new UnsupportedOperationException("Not implemented unwrap");
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
}
