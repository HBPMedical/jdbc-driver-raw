import org.json.simple.parser.ParseException;
import org.junit.Test;
import raw.jdbc.RawDatabaseMetaData;
import raw.jdbc.rawclient.requests.*;
import raw.jdbc.rawclient.RawRestClient;

import java.io.IOException;
import java.util.ArrayList;

public class TestClient extends RawTest {

    RawRestClient client;

    public TestClient() throws IOException {
        super();
        TokenResponse token = RawRestClient.getPasswdGrantToken(authServer, credentials);
        logger.fine("got token " + token.access_token);
        client = new RawRestClient(conf.getProperty("executor"), token);

    }

    @Test
    public void testVersion() throws IOException, ParseException {
        String version = client.getVersion();
        logger.fine("got version number " + version);
    }

    @Test
    public void testSchemas() throws IOException, ParseException {
        SourceNameResponse[] schemas = client.getSources();
        for (SourceNameResponse s : schemas) {
            System.out.println("got schema: " + s.name);
        }
    }

    @Test
    public void testAsyncQuery() throws IOException, ParseException {
        int id = client.asyncQueryStart("collection(1,2,4)");
        logger.fine("got queryId: " + id);
    }

    @Test
    public void testPollQuery() throws IOException, InterruptedException {
        String query = "select * from collection(1,2,3)";
        int nResults = 1000;
        AsyncQueryNextResponse results = client.pollQuery(query, nResults);
        logger.fine("results " + results.data);
        assert (results.size == 3);
        assert (results.data.getClass() == ArrayList.class);
    }

    @Test
    public void testQueryStart() throws IOException, ParseException {
        QueryBlockResponse resp = client.queryStart("collection(1,2,4)", 100);
        logger.fine("execution time: " + resp.executionTime);
    }
    //TODO: fix this test
//    @Test
//    public void testGetTabularSchemas() throws IOException, ParseException {
//        SourceNameResponse[] resp = client.getAllSourcesInfo();
//        for (SourceNameResponse source: resp) {
//            logger.fine("schema: " + source.name + "sql: " + source.);
//            RawDatabaseMetaData.SchemaInfoColumn info = schema.columns[0];
//            logger.fine("column: " + info.name + " -> " + info.tipe);
//        }
//
//    }
}
