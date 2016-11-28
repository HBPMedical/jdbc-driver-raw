import raw.jdbc.RawDriver;
import raw.jdbc.oauth2.PasswordCredentials;
import raw.jdbc.oauth2.TokenResponse;
import raw.jdbc.RawRestClient;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;
;

public class TestDriver extends RawTest {

    public void testGetToken() throws IOException {
        String authServer = conf.getProperty("auth_server");
        PasswordCredentials credentials = new PasswordCredentials(
                "raw-jdbc",
                null,
                conf.getProperty("username"),
                conf.getProperty("password")
        );

        TokenResponse token = RawRestClient.getPasswdGrantToken(authServer, credentials);
        logger.fine("token type: " + token.tokenType);
        logger.fine("token: " + token.accessToken);
        logger.fine("refresh token: " + token.refreshToken);
        logger.fine("expires in: " + token.expiresIn);

        assert (token.tokenType != null);
        assert (token.tokenType.equals("Bearer"));
        assert (token.accessToken != null);
        assert (token.refreshToken != null);
        assert (token.expiresIn != 0);
    }

    public void testParseUrl() throws SQLException{
        String executor = "localhost:54321";
        String authUrl = "http://localhost:9000/oauth2/access_token";
        String username = "user@nowhere.com";
        String password = "password";
        String url;
        try {
            url = String.format("jdbc:raw:http://%s:%s@%s?auth_url=%s",
                    URLEncoder.encode(username, "UTF8"),
                    URLEncoder.encode(password, "UTF8"),
                    executor,
                    URLEncoder.encode(authUrl, "UTF8"));
        } catch ( UnsupportedEncodingException e) {
            throw new SQLException("Unsupported encoding (UTF8) while parsing url.");
        }
        Properties info = RawDriver.parseUrl(url);
        logger.fine("url properties " + info);
        assert (info.getProperty("executor").equals("http://" + executor));
        assert (info.getProperty("auth_url").equals(authUrl));
        assert (info.getProperty("username").equals(username));
        assert (info.getProperty("password").equals(password));
    }

    public void testConnectWithUrl() throws SQLException {
        String executor = "localhost:54321";
        String authUrl = "http://localhost:9000/oauth2/access_token";
        String username = conf.getProperty("username");
        String password = conf.getProperty("password");
        String url = String.format("jdbc:raw:http://%s:%s@%s?auth_url=%s",
                URLEncoder.encode(username),
                URLEncoder.encode(password),
                executor,
                URLEncoder.encode(authUrl));
        RawDriver driver = new RawDriver();
        Properties info = new Properties();
        driver.connect(url, info);

        // alternative way of defining user info
        url = String.format("jdbc:raw:http://%s?auth_url=%s&username=%s&password=%s",
                executor,
                authUrl,
                username,
                password);
        driver.connect(url, info);
    }

    public void testConnectWithProperties() throws SQLException {

        String url = "jdbc:raw:http://localhost:54321";
        RawDriver driver = new RawDriver();
        // Will get all other settings from the property file
        driver.connect(url, conf);
    }

}
