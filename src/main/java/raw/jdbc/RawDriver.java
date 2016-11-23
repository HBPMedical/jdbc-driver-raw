package raw.jdbc;

import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import raw.jdbc.oauth2.TokenResponse;
import raw.jdbc.oauth2.OAuthUtils;
import raw.jdbc.oauth2.PasswordCredentials;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.*;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class RawDriver implements Driver {

    static final String AUTH_SERVER_URL = "http://localhost:9000/oauth2/access_token";
    static final String JDBC_CLIENT_ID = "raw-jdbc";
    static final String GRANT_TYPE = "password";
    static final String EXEC_PROP_NAME = "executor";
    static final String AUTH_PROP_NAME = "auth_url";

    static Logger logger = Logger.getLogger(RawDriver.class.getName());

    public Connection connect(String url, Properties info) throws SQLException {
        try {
            Properties props = parseProperties(url, info);
            PasswordCredentials credentials = PasswordCredentials.fromProperties(props);
            String authUrl = props.getProperty(AUTH_PROP_NAME);
            String executer = props.getProperty(EXEC_PROP_NAME);
            TokenResponse token = OAuthUtils.getPasswdGrantToken(authUrl, credentials);
            return new RawConnection(executer, authUrl, token);

        } catch (Exception e) {
            throw new SQLException(e.getMessage(), e);
        }
    }

    public static Properties parseUrl(String url) throws SQLException {
        if (!url.startsWith("jdbc:raw:")) {
            throw new SQLException("Invalid url to start with 'jdbc:raw:'");
        }
        try {
            Properties properties = new Properties();
            URI uri = new URI(url);
            List<NameValuePair> paramList = URLEncodedUtils.parse(uri, "UTF-8");
            for (NameValuePair nvp : paramList) {
                properties.put(nvp.getName(), nvp.getValue());
            }

            String executor = String.format("%s:%d", uri.getHost(), uri.getPort());
            logger.finest("executor: " + executor);
            properties.setProperty(EXEC_PROP_NAME, executor);
            String userinfo = uri.getUserInfo();
            logger.finest("user info: " + userinfo);
            return properties;
        } catch (URISyntaxException e) {
            throw new SQLException("Invalid url", e);
        }
    }

    /**
     * Parses the URL and Properties adding default values
     * the properties defined in the url have priority
     *
     * @param url  url with parameters to parse
     * @param info properties provided by the system
     * @return New properties with default values
     * @throws URISyntaxException
     */
    private static Properties parseProperties(String url, Properties info) throws SQLException {
        //Parses url parameters into a Map
        Properties urlParams = parseUrl(url);

        Properties finalInfo = new Properties();
        finalInfo.setProperty(AUTH_PROP_NAME, AUTH_SERVER_URL);

        // copies all relevant properties giving priority to the ones defined in the url
        String[] properties = {
                AUTH_PROP_NAME,
                OAuthUtils.USERNAME,
                OAuthUtils.PASSWORD
        };

        for (String prop : properties) {
            if (urlParams.containsKey(prop)) {
                finalInfo.setProperty(prop, urlParams.getProperty(prop));
            } else if (info.containsKey(prop)) {
                finalInfo.setProperty(prop, info.getProperty(prop));
            }
        }
        // sets the executor property from the url parameters
        finalInfo.setProperty(EXEC_PROP_NAME, urlParams.getProperty(EXEC_PROP_NAME));
        finalInfo.setProperty(OAuthUtils.CLIENT_ID, JDBC_CLIENT_ID);
        finalInfo.setProperty(OAuthUtils.GRANT_TYPE, GRANT_TYPE);
        if (!finalInfo.contains(AUTH_PROP_NAME)) {
            throw new SQLException("Could not find authorization url property");
        }
        return finalInfo;
    }

    public boolean acceptsURL(String url) {

        try {
            parseUrl(url);
        } catch (SQLException e) {
            return false;
        }
        return true;
    }

    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        throw new UnsupportedOperationException("not implemented");
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

    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }
}
