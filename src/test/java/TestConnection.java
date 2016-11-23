import oauth.OAuth2Details;
import oauth.OAuthUtils;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;

public class TestConnection {

    Properties credentials;

    TestConnection(){
        String path = "credentials.conf";
        InputStream is = OAuthUtils.class.getClassLoader().getResourceAsStream( path);
        credentials = new Properties();
        credentials.load(is);

    }

    public static Properties getClientConfigProps(String path) {


    @Test
    public void testGetToken() throws IOException {

        Properties info = new Properties();

        info.setProperty("authentication_server_url", "http://localhost:9000/oauth2/access_token");
        info.setProperty("client_id", "raw-jdbc");
        info.setProperty("grant_type", "password");
        info.setProperty("username", credentials.getProperty("username"));
        info.setProperty("password", credentials.getProperty("password"));

        OAuth2Details oauthDetails = OAuthUtils.createOAuthDetails(info);

        String token = OAuthUtils.getTokenClientSecret(oauthDetails );
        System.out.print("token: " + token);

    }
}
