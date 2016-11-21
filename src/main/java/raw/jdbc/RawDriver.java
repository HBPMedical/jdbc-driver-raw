package raw.jdbc;

import oauth.OAuth2Details;
import org.apache.oltu.oauth2.common.exception.OAuthProblemException;
import org.apache.oltu.oauth2.common.exception.OAuthSystemException;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;


public class RawDriver implements Driver {

    @SuppressWarnings("Since15")
    public Connection connect(String url, Properties info) throws SQLException {
        String authUri = info.getProperty("authentication_server_url");
        String clientSecret = info.getProperty("client_secret");
        String clientId = info.getProperty("client_id");
        String username = info.getProperty("username");
        String password = info.getProperty("password");

        OAuth2Details oAuth2Details = new OAuth2Details(authUri,
                );
        try {
            return new RawConnection();
        } catch (Exception e) {
            e.printStackTrace();
            throw new SQLException("Could not connect", e);
        }
    }

    private String getToken(String url, Properties info) {


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
