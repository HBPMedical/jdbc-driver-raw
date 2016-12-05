package raw.jdbc;

import org.apache.commons.lang3.ArrayUtils;
import raw.jdbc.rawclient.RawRestClient;
import raw.jdbc.rawclient.requests.QueryBlockResponse;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.logging.Logger;

public class RawResultSet implements ResultSet {
    private RawRestClient client;
    QueryBlockResponse query;

    private boolean isRecord = false;
    private String[] columnNames;
    private RawStatement statement;
    private int currentRow = -1;
    private int currentIndex = -1;
    private String sql;
    private int resultsPerPage = 1000;

    static Logger logger = Logger.getLogger(RawResultSet.class.getName());

    RawResultSet(RawRestClient client, String sql, RawStatement parent) {
        this.client = client;
        this.statement = parent;
        this.sql = sql;
    }

    private void startQuery() throws SQLException {
        try {
            query = client.queryStart(sql, resultsPerPage);
            if (query.data.length > 0) {
                Object obj = query.data[0];
                if (obj.getClass() == LinkedHashMap.class) {
                    this.isRecord = true;
                    Map<String, Object> map = (Map) obj;
                    columnNames = map.keySet().toArray(new String[]{});
                } else {
                    this.isRecord = false;
                }
            }
            logger.fine("initialized query token: " + query.token + " hasMore: " + query.hasMore);
        } catch (IOException e) {
            throw new SQLException("could not start query: " + e.getMessage());
        }

    }

    public boolean isBeforeFirst() throws SQLException {
        return currentIndex == -1;
    }

    public boolean isAfterLast() throws SQLException {
        return (currentIndex >= query.data.length) && (!query.hasMore);
    }

    public boolean isFirst() throws SQLException {
        return currentIndex == 0;
    }

    public boolean isLast() throws SQLException {
        return (currentIndex == (query.data.length - 1) && !query.hasMore);
    }

    public boolean next() throws SQLException {
        if (currentIndex == -1) {
            startQuery();
        }

        if (currentIndex < query.data.length - 1) {
            currentIndex++;
            currentRow++;
            return true;

        } else {
            if (query.hasMore) {
                try {
                    logger.fine("getting more results for query " + query.token);
                    query = client.queryNext(query.token, resultsPerPage);
                    currentIndex = 0;
                    currentRow++;
                    return true;
                } catch (IOException e) {
                    throw new SQLException("Could not get next page for query error:" + e.getMessage());
                }
            } else {
                if (currentIndex == query.data.length - 1) {
                    currentIndex++;
                }
                return false;
            }
        }
    }

    public void close() throws SQLException {
        try {
            client.queryClose(query.token);
        } catch (IOException e) {
            throw new SQLException("query-close failed: " + e.getMessage());
        }
    }

    public boolean wasNull() throws SQLException {
        return false;
    }

    private <T> T castFromColIndex(int columnIndex, Class<T> tClass) {
        if (currentIndex < 0) {
            return null;
        }
        Object obj = query.data[currentIndex];
        if (obj == null) {
            return null;
        }

        if (isRecord) {
            Map<String, Object> map = (Map) obj;
            return castToType(map.get(columnNames[columnIndex]), tClass);
            //TODO: check if this is allowed
        } else if (obj.getClass().isArray()) {
            Object[] ary = (Object[]) obj;
            return castToType(ary[columnIndex], tClass);
        } else if (obj.getClass() == ArrayList.class) {
            ArrayList<Object> ary = (ArrayList) obj;
            return castToType(ary.get(columnIndex), tClass);
        } else if (columnIndex != 0) {
            throw new IndexOutOfBoundsException("Row is not record or collection to get column index > 0");

        } else {
            return castToType(obj, tClass);
        }

    }

    private <T> T castFromColLabel(String columnLabel, Class<T> tClass) {
        if (currentIndex < 0) {
            return null;
        }
        Object obj = query.data[currentIndex];
        if (obj != null) {
            if (isRecord) {
                LinkedHashMap<String, Object> map = (LinkedHashMap) obj;
                return castToType(map.get(columnLabel), tClass);
            } else {
                throw new IndexOutOfBoundsException("column labels are only allowed in record types");
            }
        } else {
            return null;
        }
    }

    /**
     * Casts or transforms the data to array types
     *
     * @param obj    The object to cast
     * @param tClass The desired output class
     * @param <T>    the desired output class
     * @return returns the object casted
     */
    private <T> T transformArray(ArrayList obj, Class<T> tClass) {
        if (tClass == String[].class) {
            return (T) obj.toArray(new String[0]);
        } else if (tClass == Double[].class) {
            return (T) obj.toArray(new Double[0]);
        } else if (tClass == Float[].class) {
            return (T) obj.toArray(new Float[0]);
        } else if (tClass == Long[].class) {
            return (T) obj.toArray(new Long[0]);
        } else if (tClass == int[].class) {
            Integer[] l = (Integer[]) obj.toArray(new Integer[0]);
            return (T) ArrayUtils.toPrimitive(l);
        } else if (tClass == float[].class) {
            float[] arr = new float[obj.size()];
            //For floats we have to copy the full array manually
            for (int i = 0; i < obj.size(); i++) {
                arr[i] = new Float((Double) obj.get(i));
            }
            return (T) arr;
        } else if (tClass == double[].class) {
            Double[] l = (Double[]) obj.toArray(new Double[0]);
            return (T) ArrayUtils.toPrimitive(l);
        } else {
            return (T) obj;
        }
    }

    private <T> T castToType(Object obj, Class<T> tClass) {
        try {
            if (tClass == String.class) {
                return (T) obj.toString();
            } else if (tClass == float.class) {
                return (T) new Float((Double) obj);
            } else if (tClass == BigDecimal.class) {
                return (T) BigDecimal.valueOf((Double) obj);
            } else if (tClass.isArray()) {
                return transformArray((ArrayList) obj, tClass);
            } else {
                return (T) obj;
            }
        } catch (ClassCastException e) {
            throw new ClassCastException(obj.getClass().getName() + " cannot be converted to " + tClass.getName());
        }

    }

    public String getString(int columnIndex) throws SQLException {
        return castFromColIndex(columnIndex, String.class);
    }

    public boolean getBoolean(int columnIndex) throws SQLException {
        return castFromColIndex(columnIndex, boolean.class);
    }

    public byte getByte(int columnIndex) throws SQLException {
        return castFromColIndex(columnIndex, byte.class);
    }

    public short getShort(int columnIndex) throws SQLException {
        return castFromColIndex(columnIndex, short.class);
    }

    public int getInt(int columnIndex) throws SQLException {
        return castFromColIndex(columnIndex, int.class);
    }

    public long getLong(int columnIndex) throws SQLException {
        return castFromColIndex(columnIndex, long.class);
    }

    public float getFloat(int columnIndex) throws SQLException {
        return castFromColIndex(columnIndex, float.class);
    }

    public double getDouble(int columnIndex) throws SQLException {
        return castFromColIndex(columnIndex, double.class);
    }

    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        return castFromColIndex(columnIndex, BigDecimal.class);
    }

    public byte[] getBytes(int columnIndex) throws SQLException {
        return castFromColIndex(columnIndex, byte[].class);
    }

    public Date getDate(int columnIndex) throws SQLException {
        return null;
    }

    public Time getTime(int columnIndex) throws SQLException {
        return null;
    }

    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return null;
    }

    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        return null;
    }

    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        return null;
    }

    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        return null;
    }

    public String getString(String columnLabel) throws SQLException {
        return castFromColLabel(columnLabel, String.class);
    }

    public boolean getBoolean(String columnLabel) throws SQLException {
        return castFromColLabel(columnLabel, boolean.class);
    }

    public byte getByte(String columnLabel) throws SQLException {
        return castFromColLabel(columnLabel, byte.class);
    }

    public short getShort(String columnLabel) throws SQLException {
        return castFromColLabel(columnLabel, short.class);
    }

    public int getInt(String columnLabel) throws SQLException {
        return castFromColLabel(columnLabel, int.class);
    }

    public long getLong(String columnLabel) throws SQLException {
        return castFromColLabel(columnLabel, long.class);
    }

    public float getFloat(String columnLabel) throws SQLException {
        return castFromColLabel(columnLabel, float.class);
    }

    public double getDouble(String columnLabel) throws SQLException {
        return castFromColLabel(columnLabel, double.class);
    }

    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return castFromColLabel(columnLabel, BigDecimal.class);
    }

    public byte[] getBytes(String columnLabel) throws SQLException {
        return castFromColLabel(columnLabel, byte[].class);
    }

    public Date getDate(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    public Time getTime(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    public SQLWarning getWarnings() throws SQLException {
        throw new UnsupportedOperationException("Not supported");
    }

    public void clearWarnings() throws SQLException {
        throw new UnsupportedOperationException("Not supported");
    }

    public String getCursorName() throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    public ResultSetMetaData getMetaData() throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    public Object getObject(int columnIndex) throws SQLException {
        return castFromColIndex(columnIndex, Object.class);
    }

    public Object getObject(String columnLabel) throws SQLException {
        return castFromColLabel(columnLabel, Object.class);
    }

    public int findColumn(String columnLabel) throws SQLException {
        int idx = Arrays.asList(columnNames).indexOf(columnLabel);
        return idx;
    }

    public Reader getCharacterStream(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    public Reader getCharacterStream(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return castFromColIndex(columnIndex, BigDecimal.class);
    }

    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return castFromColLabel(columnLabel, BigDecimal.class);
    }

    public void beforeFirst() throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void afterLast() throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public boolean first() throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public boolean last() throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    public int getRow() throws SQLException {
        return currentRow;
    }

    public boolean absolute(int row) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public boolean relative(int rows) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public boolean previous() throws SQLException {
        if (currentIndex < 1) {
            return false;
        } else {
            currentIndex--;
            currentRow--;
            return true;
        }
    }

    public void setFetchSize(int rows) throws SQLException {
        this.resultsPerPage = rows;
    }

    public int getFetchSize() throws SQLException {
        return resultsPerPage;
    }

    public void setFetchDirection(int direction) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public int getFetchDirection() throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public int getType() throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public int getConcurrency() throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public boolean rowUpdated() throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public boolean rowInserted() throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public boolean rowDeleted() throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateNull(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateShort(int columnIndex, short x) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateInt(int columnIndex, int x) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateLong(int columnIndex, long x) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateString(int columnIndex, String x) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateDate(int columnIndex, Date x) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateTime(int columnIndex, Time x) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateNull(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateByte(String columnLabel, byte x) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateShort(String columnLabel, short x) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateInt(String columnLabel, int x) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateLong(String columnLabel, long x) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateFloat(String columnLabel, float x) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateDouble(String columnLabel, double x) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateString(String columnLabel, String x) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateDate(String columnLabel, Date x) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateTime(String columnLabel, Time x) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateObject(String columnLabel, Object x) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void insertRow() throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateRow() throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void deleteRow() throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void refreshRow() throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void cancelRowUpdates() throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void moveToInsertRow() throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void moveToCurrentRow() throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public Statement getStatement() throws SQLException {
        return this.statement;
    }

    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    public Ref getRef(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    public Blob getBlob(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("Not supported");
    }

    public Clob getClob(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("Not supported");
    }

    public Array getArray(int columnIndex) throws SQLException {
        return castFromColIndex(columnIndex, Array.class);
    }

    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        return null;
    }

    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        return castFromColIndex(columnIndex, type);
    }

    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return castFromColLabel(columnLabel, type);
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    public Ref getRef(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    public Blob getBlob(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    public Clob getClob(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    public Array getArray(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    public URL getURL(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    public URL getURL(String columnLabel) throws SQLException {
        return null;
    }

    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateRef(String columnLabel, Ref x) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateClob(String columnLabel, Clob x) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateArray(String columnLabel, Array x) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public RowId getRowId(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    public RowId getRowId(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public int getHoldability() throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public boolean isClosed() throws SQLException {
        return false;
    }

    public void updateNString(int columnIndex, String nString) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateNString(String columnLabel, String nString) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public NClob getNClob(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public NClob getNClob(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public String getNString(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    public String getNString(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw new UnsupportedOperationException("Unsupported operation");
    }
}
