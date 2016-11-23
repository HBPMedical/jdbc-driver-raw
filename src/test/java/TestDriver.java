import junit.framework.TestCase;
import raw.jdbc.RawDriver;
import raw.jdbc.oauth2.PasswordCredentials;
import raw.jdbc.oauth2.TokenResponse;
import raw.jdbc.oauth2.OAuthUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
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

        InputStream is = getInputStream("logging.properties");
        LogManager.getLogManager().readConfiguration(is);
    }

    private InputStream getInputStream(String path) {
        return TestDriver.class.getClassLoader().getResourceAsStream("test.properties");
    }

    public void testGetToken() throws IOException {
        String authServer = "http://localhost:9000/oauth2/access_token";

        Properties info = new Properties();
        info.setProperty("client_id", "raw-jdbc");
        info.setProperty("grant_type", "password");

        info.setProperty("username", conf.getProperty("username"));
        info.setProperty("password", conf.getProperty("password"));

        PasswordCredentials credentials = PasswordCredentials.fromProperties(info);
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
        String url = String.format("jdbc:raw://%s:%s@%s?auth_url=%s",
                username,
                password,
                executor,
                authUrl);
        Properties info = RawDriver.parseUrl(url);
        assert (info.getProperty("executor").equals(executor));
        assert (info.getProperty("auth_url").equals(authUrl));
        assert (info.getProperty("username").equals(username));
        assert (info.getProperty("password").equals(password));
    }
}
