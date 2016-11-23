import raw.jdbc.oauth2.PasswordCredentials;
import raw.jdbc.oauth2.TokenResponse;
import raw.jdbc.oauth2.OAuthUtils;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class TestConnection {

    Properties conf;

    TestConnection() throws IOException {
        String path = "test.conf";
        InputStream is = OAuthUtils.class.getClassLoader().getResourceAsStream( path);
        conf = new Properties();
        conf.load(is);

    }

    @Test
    public void testGetToken() throws IOException {
        String authServer ="http://localhost:9000/oauth2/access_token";

        Properties info = new Properties();
        info.setProperty("client_id", "raw-jdbc");
        info.setProperty("grant_type", "password");

        info.setProperty("username", conf.getProperty("username"));
        info.setProperty("password", conf.getProperty("password"));

        PasswordCredentials credentials = PasswordCredentials.fromProperties(info);
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
