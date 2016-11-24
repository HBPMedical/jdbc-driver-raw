import raw.jdbc.RawRestClient;
import raw.jdbc.oauth2.PasswordCredentials;
import raw.jdbc.oauth2.TokenResponse;

public class TestClient extends RawTest {

    RawRestClient client;
    @Override
    protected void setUp() throws Exception {
        super.setUp();

        String authServer = conf.getProperty("auth_server");
        PasswordCredentials credentials = new PasswordCredentials(
                "raw-jdbc",
                null,
                conf.getProperty("username"),
                conf.getProperty("password")
        );

        TokenResponse token = RawRestClient.getPasswdGrantToken(authServer, credentials);
        client = new RawRestClient( conf.getProperty("executor"), token);
    }

    public void testVersion() {
        
    }
}
