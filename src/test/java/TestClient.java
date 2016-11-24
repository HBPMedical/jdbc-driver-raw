import org.json.simple.parser.ParseException;
import raw.jdbc.RawRestClient;
import raw.jdbc.oauth2.PasswordCredentials;
import raw.jdbc.oauth2.TokenResponse;

import java.io.IOException;
import java.util.Map;

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

    public void testVersion() throws IOException, ParseException {
        String version = client.getVersion();
        logger.fine("got version number " + version);
    }

    public void testSchemas() throws IOException, ParseException {
        String[] schemas = client.getSchemas();
        for(String s: schemas) {
            System.out.println("got schema: " + s);
        }
    }

    public void testAsyncQuery() throws IOException, ParseException {
        Long id = client.asyncQueryStart("collection(1,2,4)");
        logger.fine("got queryId: " + id);
    }

    public void testQueryStart() throws IOException, ParseException {
        Map results = client.queryStart("collection(1,2,4)", 100);
        logger.fine("got results: " + results);
    }
}
