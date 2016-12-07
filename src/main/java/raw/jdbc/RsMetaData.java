package raw.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Logger;

public class RsMetaData implements ResultSetMetaData {

    protected String[] columnNames;
    protected int[] types;
    static Logger logger = Logger.getLogger(RsMetaData.class.getName());

    RsMetaData(String[] names, int[] types) throws SQLException {
        this.columnNames = names;
        this.types = types;

    }

    static public int objToType(Object obj) throws SQLException {
        if (obj == null){
            return Types.NULL;
        }else if (obj.getClass() == Integer.class) {
            return Types.INTEGER;
        } else if (obj.getClass() == Long.class) {
            return Types.BIGINT;
        } else if (obj.getClass() == String.class) {
            return Types.VARCHAR;
        } else if (obj.getClass() == Double.class) {
            return Types.DOUBLE;
        } else if (obj.getClass() == Boolean.class) {
            return Types.BOOLEAN;
        } else if (obj.getClass() == LinkedHashMap.class) {
            return Types.STRUCT;
        } else if (obj.getClass() == ArrayList.class) {
            return Types.ARRAY;
        } else {
            throw new SQLException("Unsupported type " + obj.getClass());
        }
    }

    public String getColumnTypeName(int column) throws SQLException {
        int type = getColumnType(column);
        switch (type) {
            case Types.INTEGER:
                return "int";
            case Types.BIGINT:
                return "long";
            case Types.VARCHAR:
                return "string";
            case Types.DOUBLE:
                return "double";
            case Types.STRUCT:
                return "record";
            case Types.ARRAY:
                return "collection";
            case Types.BOOLEAN:
                return "boolean";
            default:
                return "unknow type";
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
        return columnNames[column - 1];
    }

    public String getColumnName(int column) throws SQLException {
        return columnNames[column - 1];
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
        return types[column - 1];
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
