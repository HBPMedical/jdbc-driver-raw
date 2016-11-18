package raw.jdbc;

import org.apache.oltu.oauth2.common.exception.OAuthProblemException;
import org.apache.oltu.oauth2.common.exception.OAuthSystemException;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;


public class RawDriver implements Driver {

    @SuppressWarnings("Since15")
    public Connection connect(String url, Properties info) throws SQLException {
        String oAuthUri = info.getProperty("oAuthUri");
        String redirectUri = info.getProperty("redirectUri");
        String userId = info.getProperty("userId");

        try {
            return new RawConnection(url, oAuthUri, redirectUri, userId);
        } catch (Exception e) {
            e.printStackTrace();
            throw new SQLException("Could not connect", e);
        }
    }

    public boolean acceptsURL(String url) throws SQLException {
        throw new UnsupportedOperationException("not implemented");
    }

    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    public int getMajorVersion() {
        return 0;
    }

    public int getMinorVersion() {
        return 0;
    }

    public boolean jdbcCompliant() {
        return false;
    }

    @SuppressWarnings("Since15")
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }
}
