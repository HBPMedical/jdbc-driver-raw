package raw.jdbc;

import oauth.OAuth2Details;
import oauth.OAuthConstants;
import oauth.OAuthUtils;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.oltu.oauth2.common.exception.OAuthProblemException;
import org.apache.oltu.oauth2.common.exception.OAuthSystemException;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;


public class RawDriver implements Driver {

    @SuppressWarnings("Since15")
    public Connection connect(String url, Properties info) throws SQLException {
        try {
            OAuth2Details oauthDetails = getOauthDetails(url, info);

            return new RawConnection(oauthDetails);

        } catch (Exception e) {
            throw new SQLException(e.getMessage(), e);
        }
    }

    private static OAuth2Details getOauthDetails(String url, Properties info) throws URISyntaxException {
        //Parses url parameters into a Map
        Map<String, String> params = new HashMap<String, String>();
        List<NameValuePair> paramList = URLEncodedUtils.parse(new URI(url), "UTF-8");
        for (NameValuePair nvp : paramList) {
            params.put(nvp.getName(), nvp.getValue());
        }

        String[] properties = {
                OAuthConstants.AUTHENTICATION_SERVER_URL,
                OAuthConstants.GRANT_TYPE,
                OAuthConstants.SCOPE,
                OAuthConstants.CLIENT_ID,
                OAuthConstants.CLIENT_SECRET,
                OAuthConstants.USERNAME,
                OAuthConstants.PASSWORD
        };

        // copies all relevant properties giving priority to the ones defined in the url
        Properties finalInfo = new Properties();
        for (String prop : properties) {
            if (params.containsKey(prop)) {
                finalInfo.setProperty(prop, params.get(prop));
            } else if (info.containsKey(prop)) {
                finalInfo.setProperty(prop, info.getProperty(prop));
            }
        }
        return OAuthUtils.createOAuthDetails(finalInfo);
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
