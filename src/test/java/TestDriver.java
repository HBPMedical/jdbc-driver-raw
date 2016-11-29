import junit.framework.TestCase;
import org.junit.Test;
import raw.jdbc.RawDriver;
import raw.jdbc.rawclient.requests.PasswordTokenRequest;
import raw.jdbc.rawclient.requests.TokenResponse;
import raw.jdbc.rawclient.RawRestClient;

import java.io.IOException;
import java.net.URLEncoder;
import java.sql.SQLException;
import java.util.Properties;
;

public class TestDriver extends RawTest {

    @Test
    public void testGetToken() throws IOException {
        String authServer = conf.getProperty("auth_server");
        PasswordTokenRequest credentials = new PasswordTokenRequest();

        credentials.client_id = "raw-jdbc";
        credentials.grant_type = "password";
        credentials.username = conf.getProperty("username");
        credentials.password = conf.getProperty("password");

        TokenResponse token = RawRestClient.getPasswdGrantToken(authServer, credentials);
        logger.fine("token type: " + token.token_type);
        logger.fine("token: " + token.access_token);
        logger.fine("refresh token: " + token.refresh_token);
        logger.fine("expires in: " + token.expires_in);

        assert (token.token_type != null);
        assert (token.token_type.equals("Bearer"));
        assert (token.token_type != null);
        assert (token.refresh_token != null);
        assert (token.expires_in != 0);
    }

    @Test
    public void testParseUrl() throws SQLException {
        String executor = "localhost:54321";
        String authUrl = "http://localhost:9000/oauth2/access_token";
        String username = "user@nowhere.com";
        String password = "password";
        String url = String.format("jdbc:raw:http://%s:%s@%s?auth_url=%s",
                URLEncoder.encode(username),
                URLEncoder.encode(password),
                executor,
                URLEncoder.encode(authUrl));
        Properties info = RawDriver.parseUrl(url);
        logger.fine("url properties " + info);
        assert (info.getProperty("executor").equals("http://" + executor));
        assert (info.getProperty("auth_url").equals(authUrl));
        assert (info.getProperty("username").equals(username));
        assert (info.getProperty("password").equals(password));
    }

    @Test
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

    @Test
    public void testConnectWithProperties() throws SQLException {

        String url = "jdbc:raw:http://localhost:54321";
        RawDriver driver = new RawDriver();
        // Will get all other settings from the property file
        driver.connect(url, conf);
    }

}
