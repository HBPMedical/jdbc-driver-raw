package raw.jdbc;

import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import raw.jdbc.oauth2.OAuthConstants;
import raw.jdbc.oauth2.TokenResponse;
import raw.jdbc.oauth2.OAuthUtils;
import raw.jdbc.oauth2.PasswordCredentials;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.*;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;


public class RawDriver implements Driver {

    static final String AUTH_SERVER_URL = "http://localhost:9000/oauth2/access_token";
    static final String JDBC_CLIENT_ID = "raw-jdbc";
    static final String EXE_PROP_NAME = "executor";
    static final String AUTH_PROP_NAME = "auth_server";

    @SuppressWarnings("Since15")
    public Connection connect(String url, Properties info) throws SQLException {
        try {
            Properties props = parseProperties(url, info);
            PasswordCredentials credentials = OAuthUtils.createCredentials(props);
            String authUrl = props.getProperty(AUTH_PROP_NAME);
            String executer = props.getProperty(EXE_PROP_NAME);
            TokenResponse token = OAuthUtils.getPasswdGrantToken(authUrl, credentials);
            return new RawConnection(executer, authUrl, token);

        } catch (Exception e) {
            throw new SQLException(e.getMessage(), e);
        }
    }

    static Properties parseUrl(String url) throws URISyntaxException {
        if (!url.startsWith("jdbc:raw:")){
            throw new URISyntaxException(url, " Invalid jdbc url for, expected url to start with 'jdbc:raw:'");
        }
        Properties properties = new Properties();
        URI uri = new URI(url);
        List<NameValuePair> paramList = URLEncodedUtils.parse(uri, "UTF-8");
        for (NameValuePair nvp : paramList) {
            properties.put(nvp.getName(), nvp.getValue());
        }

        String executer = String.format("%s:%d", uri.getHost(), uri.getPort());
        properties.setProperty(EXE_PROP_NAME, executer);
        uri.getUserInfo();

        return properties;
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
    private static Properties parseProperties(String url, Properties info) throws URISyntaxException {
        //Parses url parameters into a Map
        Properties urlParams = parseUrl(url);

        Properties finalInfo = new Properties();
        finalInfo.setProperty(AUTH_PROP_NAME, AUTH_SERVER_URL);

        // copies all relevant properties giving priority to the ones defined in the url
        String[] properties = {
                EXE_PROP_NAME,
                AUTH_PROP_NAME,
                OAuthConstants.USERNAME,
                OAuthConstants.PASSWORD
        };

        for (String prop : properties) {
            if (urlParams.containsKey(prop)) {
                finalInfo.setProperty(prop, urlParams.getProperty(prop));
            } else if (info.containsKey(prop)) {
                finalInfo.setProperty(prop, info.getProperty(prop));
            }
        }
        finalInfo.setProperty(EXE_PROP_NAME, urlParams.getProperty(EXE_PROP_NAME));
        finalInfo.setProperty(OAuthConstants.CLIENT_ID, JDBC_CLIENT_ID);
        finalInfo.setProperty(OAuthConstants.GRANT_TYPE, OAuthConstants.PASSWORD);
        return finalInfo;
    }

    public boolean acceptsURL(String url) throws SQLException {

        throw new UnsupportedOperationException("not implemented");
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

    @SuppressWarnings("Since15")
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }
}
