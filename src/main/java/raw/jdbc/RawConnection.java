package raw.jdbc;

import raw.jdbc.rawclient.RawRestClient;

import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

public class RawConnection implements Connection {
    private RawRestClient client;
    private String url;
    private String username;

    RawConnection(String url, RawRestClient client, String user) throws SQLException {
        this.url = url;
        this.client = client;
        this.username = user;
    }

    public Statement createStatement() throws SQLException {
        return new RawStatement(client, this);
    }

    public PreparedStatement prepareStatement(String sql) throws SQLException {
        throw new UnsupportedOperationException("not implemented PreparedStatement");
    }

    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new UnsupportedOperationException("not implemented CallableStatement");
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return new RawStatement(client, this);
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new UnsupportedOperationException("not implemented PreparedStatement");
    }

    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new UnsupportedOperationException("not implemented CallableStatement");
    }

    public String nativeSQL(String sql) throws SQLException {
        throw new UnsupportedOperationException("not implemented nativeSQL");
    }

    public void setAutoCommit(boolean autoCommit) throws SQLException {

    }

    public boolean getAutoCommit() throws SQLException {
        return false;
    }

    public void commit() throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported commit");
    }

    public void rollback() throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported rollback");
    }

    public void close() throws SQLException {

    }

    public boolean isClosed() throws SQLException {
        return false;
    }

    public DatabaseMetaData getMetaData() throws SQLException {
        return new RawDatabaseMetaData(url, username, client, this);
    }

    public void setReadOnly(boolean readOnly) throws SQLException {
        throw new SQLFeatureNotSupportedException("not implemented setReadOnly");
    }

    public boolean isReadOnly() throws SQLException {
        return true;
    }

    public void setCatalog(String catalog) throws SQLException {
        throw new SQLFeatureNotSupportedException("not implemented setCatalog");
    }

    public String getCatalog() throws SQLException {
        return null;
    }

    public void setTransactionIsolation(int level) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported setTransactionIsolation");
    }

    public int getTransactionIsolation() throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported getTransactionIsolation");
    }

    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    public void clearWarnings() throws SQLException {

    }

    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new SQLFeatureNotSupportedException("not implemented");
    }

    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("not implemented setTypeMap");
    }

    public void setHoldability(int holdability) throws SQLException {
        throw new SQLFeatureNotSupportedException("not implemented setHoldability");
    }

    public int getHoldability() throws SQLException {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    public Savepoint setSavepoint() throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported setSavepoint");
    }

    public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported setSavepoint");
    }

    public void rollback(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported rollback");
    }

    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("not implemented releaseSavepoint");
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new UnsupportedOperationException("not implemented createStatement");
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new UnsupportedOperationException("not implemented prepareStatement");
    }

    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new UnsupportedOperationException("not implemented prepareCall");
    }

    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        throw new UnsupportedOperationException("not implemented prepareStatement");
    }

    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        throw new UnsupportedOperationException("not implemented prepareStatement");
    }

    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        throw new UnsupportedOperationException("not implemented prepareStatement");
    }

    public Clob createClob() throws SQLException {
        throw new SQLFeatureNotSupportedException("not implemented createClob");
    }

    public Blob createBlob() throws SQLException {
        throw new SQLFeatureNotSupportedException("not implemented createBlob");
    }

    @SuppressWarnings("Since15")
    public NClob createNClob() throws SQLException {
        throw new SQLFeatureNotSupportedException("not implemented createNClob");
    }

    @SuppressWarnings("Since15")
    public SQLXML createSQLXML() throws SQLException {
        throw new SQLFeatureNotSupportedException("not implemented createSQLXML");
    }

    public boolean isValid(int timeout) throws SQLException {
        return false;
    }

    @SuppressWarnings("Since15")
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        throw new UnsupportedOperationException("not implemented setClientInfo " +
                "name: " + name + " value: " + value);
    }

    @SuppressWarnings("Since15")
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        throw new UnsupportedOperationException("not implemented setClientInfo");
    }

    public String getClientInfo(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException("not implemented getClientInfo");
    }

    public Properties getClientInfo() throws SQLException {
        throw new SQLFeatureNotSupportedException("not implemented getClientInfo");
    }

    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new SQLFeatureNotSupportedException("not implemented createArrayOf");
    }

    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new UnsupportedOperationException("not implemented createStruct");
    }

    public void setSchema(String schema) throws SQLException {
        throw new UnsupportedOperationException("not implemented setSchema");
    }

    public String getSchema() throws SQLException {
        return null;
    }

    public void abort(Executor executor) throws SQLException {
        throw new UnsupportedOperationException("not implemented abort");
    }

    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        throw new UnsupportedOperationException("not implemented setNetworkTimeout");
    }

    public int getNetworkTimeout() throws SQLException {
        throw new UnsupportedOperationException("not implemented getNetworkTimeout");
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new UnsupportedOperationException("not implemented");
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
}
