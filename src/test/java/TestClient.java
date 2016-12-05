import org.json.simple.parser.ParseException;
import org.junit.Test;
import raw.jdbc.rawclient.requests.QueryBlockResponse;
import raw.jdbc.rawclient.RawRestClient;
import raw.jdbc.rawclient.requests.TokenResponse;

import java.io.IOException;

public class TestClient extends RawTest {

    RawRestClient client;

    public TestClient() throws IOException {
        super();
        TokenResponse token = RawRestClient.getPasswdGrantToken(authServer, credentials);
        client = new RawRestClient(conf.getProperty("executor"), token);
    }

    @Test
    public void testVersion() throws IOException, ParseException {
        String version = client.getVersion();
        logger.fine("got version number " + version);
    }

    @Test
    public void testSchemas() throws IOException, ParseException {
        String[] schemas = client.getSchemas();
        for (String s : schemas) {
            System.out.println("got schema: " + s);
        }
    }

    @Test
    public void testAsyncQuery() throws IOException, ParseException {
        int id = client.asyncQueryStart("collection(1,2,4)");
        logger.fine("got queryId: " + id);
    }

    @Test
    public void testQueryStart() throws IOException, ParseException {
        QueryBlockResponse resp = client.queryStart("collection(1,2,4)", 100);
        logger.fine("execution time: " + resp.executionTime);
    }
}
