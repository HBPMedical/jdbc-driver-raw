import junit.framework.TestCase;
import raw.jdbc.RawDriver;
import raw.jdbc.oauth2.PasswordCredentials;
import raw.jdbc.oauth2.TokenResponse;
import raw.jdbc.oauth2.OAuthUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.LogManager;
import java.util.logging.Logger;
;

public class TestDriver extends TestCase {

    protected Properties conf;
    static Logger logger = Logger.getLogger(TestDriver.class.getName());

    public TestDriver() throws IOException {
        conf = new Properties();
        conf.load(getInputStream("test.properties"));

        LogManager.getLogManager()
                .readConfiguration(getInputStream("test.properties"));
    }

    private InputStream getInputStream(String path) {
        return TestDriver.class.getClassLoader().getResourceAsStream(path);
    }

    public void testGetToken() throws IOException {
        String authServer = "http://localhost:9000/oauth2/access_token";
        PasswordCredentials credentials = new PasswordCredentials(
                "raw-jdbc",
                null,
                conf.getProperty("username"),
                conf.getProperty("password")
        );

        TokenResponse token = OAuthUtils.getPasswdGrantToken(authServer, credentials);
        logger.fine("token type: " + token.tokenType);
        logger.fine("token: " + token.acessToken);
        logger.fine("refresh token: " + token.refreshToken);
        logger.fine("expires in: " + token.expiresIn);

        assert (token.tokenType != null);
        assert (token.tokenType.equals("Bearer"));
        assert (token.acessToken != null);
        assert (token.refreshToken != null);
        assert (token.expiresIn != 0);
    }

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
        Properties info  = new Properties();
        driver.connect(url, info);
    }

    public void testConnectWithProperties() throws SQLException {

        String url ="jdbc:raw:http://localhost:54321";
        RawDriver driver = new RawDriver();
        // Will get the user credentials from the properties file
        driver.connect(url, conf);
    }

}
