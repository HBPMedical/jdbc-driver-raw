import org.junit.Test;
import raw.jdbc.RawDriver;
import raw.jdbc.rawclient.requests.PasswordTokenRequest;
import raw.jdbc.rawclient.requests.TokenResponse;
import raw.jdbc.rawclient.RawRestClient;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
;

public class TestDriver extends RawTest {


    @Test
    public void testParseUrl() throws SQLException {
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
        assert (info.getProperty("user").equals(username));
        assert (info.getProperty("password").equals(password));
    }

    @Test
    public void testConnectWithUrl() throws SQLException, UnsupportedEncodingException {
        String executor = "localhost:54321";
        String authUrl = "http://localhost:9000/oauth2/access_token";
        String username = this.credentials.username;
        String password = this.credentials.password;
        String url = String.format("jdbc:raw:http://%s:%s@%s?auth_url=%s",
                URLEncoder.encode(username, "UTF8"),
                URLEncoder.encode(password, "UTF8"),
                executor,
                URLEncoder.encode(authUrl));
        RawDriver driver = new RawDriver();
        Properties info = new Properties();
        driver.connect(url, info);

        // alternative way of defining user info
        url = String.format("jdbc:raw:http://%s?auth_url=%s&user=%s&password=%s",
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

    @Test
    public void testDriverManager() throws SQLException {
        RawDriver.register();
        String url = "jdbc:raw:http://localhost:54321";
        Driver driver = DriverManager.getDriver(url);
        assert(driver.getClass() == RawDriver.class);
    }
}
