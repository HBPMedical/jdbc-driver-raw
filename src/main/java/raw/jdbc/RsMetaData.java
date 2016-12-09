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

    public String getColumnTypeName(int column) throws SQLException {
        int type = getColumnType(column);
        return RawDatabaseMetaData.typeToName(type);
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
        return columnNoNulls;
    }

    public boolean isSigned(int column) throws SQLException {
        return false;
    }

    public int getColumnDisplaySize(int column) throws SQLException {
        return 100;
    }

    public String getColumnLabel(int column) throws SQLException {
        if (column < 1 || column > columnNames.length) {
            throw new SQLException("index out of bounds " + column);
        }
        return columnNames[column - 1];
    }

    public String getColumnName(int column) throws SQLException {
        if (column < 1 || column > columnNames.length) {
            throw new SQLException("index out of bounds " + column);
        }
        System.out.println("column: " + column + " name: " + columnNames[column - 1]);
        return columnNames[column - 1];
    }

    public String getSchemaName(int column) throws SQLException {
        return "unknown";
    }

    public int getPrecision(int column) throws SQLException {
        return 1;
    }

    public int getScale(int column) throws SQLException {
        return 1;
    }

    public String getTableName(int column) throws SQLException {
        return null;
    }

    public String getCatalogName(int column) throws SQLException {
        return null;
    }

    public int getColumnType(int column) throws SQLException {
        if (column < 1 || column > types.length) {
            throw new SQLException("index out of bounds " + column);
        }
        System.out.println("column: " + column + " type: " + types[column - 1]);
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
