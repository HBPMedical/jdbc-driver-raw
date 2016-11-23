import raw.jdbc.oauth2.PasswdCredentials;
import raw.jdbc.oauth2.TokenResponse;
import raw.jdbc.oauth2.OAuthUtils;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class TestConnection {

    Properties credentials;

    TestConnection() throws IOException {
        String path = "credentials.conf";
        InputStream is = OAuthUtils.class.getClassLoader().getResourceAsStream( path);
        credentials = new Properties();
        credentials.load(is);

    }

    @Test
    public void testGetToken() throws IOException {
        String authServer ="http://localhost:9000/oauth2/access_token";

        Properties info = new Properties();
        info.setProperty("client_id", "raw-jdbc");
        info.setProperty("grant_type", "password");

        info.setProperty("username", credentials.getProperty("username"));
        info.setProperty("password", credentials.getProperty("password"));

        TokenResponse token = OAuthUtils.getPasswdGrantToken(authServer, credentials);
        System.out.println("token type: " + token.tokenType);
        System.out.println("token: " + token.acessToken);
        System.out.println("refresh token: " + token.refreshToken);
        System.out.println("expires in: " + token.expiresIn);

        assert(token.tokenType.equals("Bearer"));
        assert(token.acessToken != null);
        assert(token.refreshToken != null);
        assert(token.expiresIn != 0);
    }

    @Test
    public void getConnection() {
        
    }
}
