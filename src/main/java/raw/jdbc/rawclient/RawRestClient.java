package raw.jdbc.rawclient;

import java.io.IOException;
import java.util.LinkedList;
import java.util.logging.Logger;

import com.fasterxml.jackson.core.JsonParseException;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import raw.jdbc.rawclient.requests.*;
import java.util.ArrayList;

public class RawRestClient {
    private static Logger logger = Logger.getLogger(RawRestClient.class.getName());
    private static ObjectMapper mapper = new ObjectMapper();

    private static final String JSON_CONTENT = "application/json";
    private static final int HTTP_OK = 200;

    private HttpClient client = HttpClientBuilder.create().build();
    private String executerUrl;
    private TokenResponse credentials;

    public RawRestClient(String executor, TokenResponse token) {
        this.credentials = token;
        this.executerUrl = executor;
    }

    public RawRestClient(String executor, String authUrl, PasswordTokenRequest credentials) throws IOException {
        this(executor, getPasswdGrantToken(authUrl, credentials));
    }

    /**
     * Gets a token using password authentication to an oauth2 server
     *
     * @param credentials
     * @return Return token response
     * @throws IOException
     */
    public static TokenResponse getPasswdGrantToken(String authUrl, PasswordTokenRequest credentials) throws IOException {
        HttpClient client = HttpClientBuilder.create().build();
        HttpPost post = new HttpPost(authUrl);

        String json = mapper.writeValueAsString(credentials);
        StringEntity entity = new StringEntity(json, "UTF-8");

        entity.setContentType("application/json");
        post.setEntity(entity);

        HttpResponse response = client.execute(post);
        int code = response.getStatusLine().getStatusCode();
        if (code != HTTP_OK) {
            String content = EntityUtils.toString(response.getEntity());
            logger.warning("Token request failed with code: " + code + " response: " + content);
            throw new RawClientException("Token request failed with code " + code);
        }
        String jsonStr = getJsonContent(response);
        return mapper.readValue(jsonStr, TokenResponse.class);
    }

    public String getVersion() throws IOException {
        HttpResponse response = doGet("/version");
        String contentType = response.getEntity().getContentType().getValue();
        assert (contentType.contains("text/plain"));
        return EntityUtils.toString(response.getEntity());
    }

    public SourceNameResponse getSourceInfo(String name) throws IOException {
        SourceNameResponse data = doGet("/sources/" + name, SourceNameResponse.class);
        return data;
    }


    public SourceNameResponse[] getSources() throws IOException {
        SourceNameResponse[] data = doGet("/sources",SourceNameResponse[].class);
        return data;
    }

    public int asyncQueryStart(String query) throws IOException {
        AsyncQueryRequest request = new AsyncQueryRequest();
        request.query = query;
        AsyncQueryResponse data = doJsonPost("/async-query-start", request, AsyncQueryResponse.class);
        return data.queryId;
    }

    private AsyncQueryNextResponse asyncQueryNext(AsyncQueryNextRequest request) throws IOException {
        return doJsonPost("/async-query-next", request, AsyncQueryNextResponse.class);
    }

    public AsyncQueryNextResponse asyncQueryNext(int queryId, int numberOfResults) throws IOException {
        AsyncQueryNextRequest request = new AsyncQueryNextRequest();
        request.queryId = queryId;
        request.numberResults = numberOfResults;
        return asyncQueryNext(request);
    }

    public void asyncQueryClose(int queryId) throws IOException {
        AsyncQueryCloseRequest request = new AsyncQueryCloseRequest();
        request.queryId = queryId;
        doJsonPost("/async-query-close", request, AsyncQueryCloseResponse.class);
    }

    public AsyncQueryNextResponse pollQuery(String query, int numberOfResults) throws IOException {
        int queryId = asyncQueryStart(query);
        AsyncQueryNextRequest request = new AsyncQueryNextRequest();
        request.queryId = queryId;
        request.numberResults = numberOfResults;
        //TODO: Add some sort of timeout for query execution
        while (true) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RawClientException("sleep interruped while polling " + e.getMessage());
            }
            AsyncQueryNextResponse data = asyncQueryNext(request);
            if (data.size > 0) {
                logger.fine("got results: compilation time: " + data.compilationTime +
                        " execution time: " + data.executionTime);
                return data;
            }
        }
    }

    public QueryBlockResponse queryStart(String query, int resultsPerPage) throws IOException {
        QueryStartRequest request = new QueryStartRequest();
        request.query = query;
        request.resultsPerPage = resultsPerPage;
        return doJsonPost("/query-start", request, QueryBlockResponse.class);
    }

    public QueryBlockResponse queryNext(String queryToken, int resultsPerPage) throws IOException {
        QueryNextRequest request = new QueryNextRequest();
        request.token = queryToken;
        request.resultsPerPage = resultsPerPage;
        return doJsonPost("/query-next", request, QueryBlockResponse.class);
    }

    public void queryClose(String token) throws IOException {
        QueryCloseRequest request = new QueryCloseRequest();
        request.token = token;
        String json = mapper.writeValueAsString(request);
        HttpResponse response = doJsonPost("/query-close", json);
        String content = EntityUtils.toString(response.getEntity());
        logger.warning("query-close response: " + content);
    }

    private static String getJsonContent(HttpResponse response) throws IOException {
        String contentType = response.getEntity().getContentType().getValue();
        if (!contentType.contains(JSON_CONTENT)) {
            throw new RawClientException(
                    "Expected content with '" + JSON_CONTENT + "' but got instead " + contentType);
        }
        return EntityUtils.toString(response.getEntity());
    }

    private HttpResponse doJsonPost(String path, String json) throws IOException {
        HttpPost post = new HttpPost(executerUrl + path);
        post.setHeader("Authorization", "Bearer " + credentials.access_token);
        StringEntity entity = new StringEntity(json, "UTF-8");
        entity.setContentType("application/json");
        post.setEntity(entity);
        return client.execute(post);
    }

    private <T> T doJsonPost(String path, Object request, Class<T> responseClass) throws IOException {
        String json = mapper.writeValueAsString(request);
        HttpResponse response = doJsonPost(path, json);
        int code = response.getStatusLine().getStatusCode();
        if (code != HTTP_OK) {
            String content = EntityUtils.toString(response.getEntity());
            try {
                GenericErrorResponse error = mapper.readValue(content, GenericErrorResponse.class);
                throw new RawClientException(error.errorType + ": " + error.errorDescription);
            } catch (JsonParseException e) {
                throw new RawClientException(path + " request failed with code " + code +
                        " content: " + content);
            }
        }
        String data = getJsonContent(response);
        return mapper.readValue(data, responseClass);
    }

    private HttpResponse doGet(String path) throws IOException {
        String url = executerUrl + path;
        logger.fine("sending get to " + url);
        HttpGet get = new HttpGet(url);
        get.setHeader("Authorization", "Bearer " + credentials.access_token);
        return client.execute(get);
    }

    private <T> T doGet(String path, Class<T> tClass) throws IOException {
        HttpResponse response = doGet(path);
        String json = getJsonContent(response);
        logger.fine("got response:\n" + json);
        return mapper.readValue(json, tClass);
    }

}