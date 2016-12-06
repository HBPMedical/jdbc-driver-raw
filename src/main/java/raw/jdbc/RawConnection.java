package raw.jdbc;

import raw.jdbc.rawclient.requests.PasswordTokenRequest;
import raw.jdbc.rawclient.requests.TokenResponse;
import raw.jdbc.rawclient.RawRestClient;

import java.io.IOException;
import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

public class RawConnection implements Connection {
    private String authUrl;
    private RawRestClient client;
    private Properties properties;
    private String url;

    RawConnection(String url, Properties props) throws SQLException {
        this.properties = props;
        this.url = url;

        PasswordTokenRequest credentials = new PasswordTokenRequest();
        credentials.client_id = RawDriver.JDBC_CLIENT_ID;
        credentials.client_secret = null;
        credentials.grant_type = RawDriver.GRANT_TYPE;
        credentials.username = props.getProperty(RawDriver.USER_PROPERTY);
        credentials.password = props.getProperty(RawDriver.PASSWD_PROPERTY);

        try {
            String authUrl = properties.getProperty(RawDriver.AUTH_PROPERTY);
            String executor = properties.getProperty(RawDriver.EXEC_PROPERTY);
            TokenResponse token = RawRestClient.getPasswdGrantToken(authUrl, credentials);
            this.authUrl = authUrl;
            this.client = new RawRestClient(executor, token);
        } catch (IOException e) {
            throw new SQLException("Unable to get bearer token for jdbc connection: " + e.getMessage());
        }

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

    }

    public void rollback() throws SQLException {

    }

    public void close() throws SQLException {

    }

    public boolean isClosed() throws SQLException {
        return false;
    }

    public DatabaseMetaData getMetaData() throws SQLException {
        String user = this.properties.getProperty(RawDriver.USER_PROPERTY);
        return new RawDatabaseMetaData(url, user, client);
    }

    public void setReadOnly(boolean readOnly) throws SQLException {
        throw new UnsupportedOperationException("not implemented setReadOnly");
    }

    public boolean isReadOnly() throws SQLException {
        return true;
    }

    public void setCatalog(String catalog) throws SQLException {
        throw new UnsupportedOperationException("not implemented setCatalog");
    }

    public String getCatalog() throws SQLException {
        try {
            String[] schemas = client.getSchemas();
            return schemas.toString();
        } catch (IOException e) {
            throw new SQLException("Error getting schemas: "+ e.getMessage());
        }
    }

    public void setTransactionIsolation(int level) throws SQLException {
        throw new UnsupportedOperationException("not implemented setTransactionIsolation");
    }

    public int getTransactionIsolation() throws SQLException {
        throw new UnsupportedOperationException("not implemented getTransactionIsolation");
    }

    public SQLWarning getWarnings() throws SQLException {
        throw new UnsupportedOperationException("not implemented getWarnings");
    }

    public void clearWarnings() throws SQLException {
        throw new UnsupportedOperationException("not implemented clearWarnings");
    }

    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new UnsupportedOperationException("not implemented");
    }

    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new UnsupportedOperationException("not implemented setTypeMap");
    }

    public void setHoldability(int holdability) throws SQLException {
        throw new UnsupportedOperationException("not implemented setHoldability");
    }

    public int getHoldability() throws SQLException {
        throw new UnsupportedOperationException("not implemented getHoldability");
    }

    public Savepoint setSavepoint() throws SQLException {
        throw new UnsupportedOperationException("not supported setSavepoint");
    }

    public Savepoint setSavepoint(String name) throws SQLException {
        throw new UnsupportedOperationException("not supported setSavepoint");
    }

    public void rollback(Savepoint savepoint) throws SQLException {
        throw new UnsupportedOperationException("not supported rollback");
    }

    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new UnsupportedOperationException("not implemented releaseSavepoint");
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
        throw new UnsupportedOperationException("not implemented createClob");
    }

    public Blob createBlob() throws SQLException {
        throw new UnsupportedOperationException("not implemented createBlob");
    }

    @SuppressWarnings("Since15")
    public NClob createNClob() throws SQLException {
        throw new UnsupportedOperationException("not implemented createNClob");
    }

    @SuppressWarnings("Since15")
    public SQLXML createSQLXML() throws SQLException {
        throw new UnsupportedOperationException("not implemented createSQLXML");
    }

    public boolean isValid(int timeout) throws SQLException {
        return false;
    }

    @SuppressWarnings("Since15")
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        throw new UnsupportedOperationException("not implemented setClientInfo " +
        "name: "+ name + " value: " + value);
    }

    @SuppressWarnings("Since15")
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        throw new UnsupportedOperationException("not implemented setClientInfo");
    }

    public String getClientInfo(String name) throws SQLException {
        throw new UnsupportedOperationException("not implemented getClientInfo");
    }

    public Properties getClientInfo() throws SQLException {
        throw new UnsupportedOperationException("not implemented getClientInfo");
    }

    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new UnsupportedOperationException("not implemented createArrayOf");
    }

    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new UnsupportedOperationException("not implemented createStruct");
    }

    public void setSchema(String schema) throws SQLException {
        throw new UnsupportedOperationException("not implemented setSchema");
    }

    public String getSchema() throws SQLException {
        throw new UnsupportedOperationException("not implemented getSchema");
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
