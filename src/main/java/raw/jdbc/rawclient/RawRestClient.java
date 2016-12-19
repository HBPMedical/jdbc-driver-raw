package raw.jdbc.rawclient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import raw.jdbc.rawclient.requests.*;

public class RawRestClient {

    private static final String JSON_CONTENT = "application/json";
    private static final int HTTP_OK = 200;
    private static final int BAD_REQUEST = 400;
    private static final int UNAUTHORIZED = 401;
    private static final int FORBIDEN = 403;
    private static final int INTERNAL_SERVER_ERROR = 501;

    private static Logger logger = Logger.getLogger(RawRestClient.class.getName());
    private static ObjectMapper mapper = new ObjectMapper();

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
        TokenResponse oauthToken = mapper.readValue(jsonStr, TokenResponse.class);
        return oauthToken;
    }

    public String getVersion() throws IOException {
        HttpResponse response = doGet("/version");
        String contentType = response.getEntity().getContentType().getValue();
        assert (contentType.contains("text/plain"));
        String data = EntityUtils.toString(response.getEntity());
        return data;
    }

    public SchemaInfo[] getSchemaInfo() throws IOException {
        SchemaInfoResponse data = doGet("/schema-info", SchemaInfoResponse.class);
        return data.schemas;
    }

    public String[] getSchemas() throws IOException {
        SchemasResponse data = doGet("/schemas", SchemasResponse.class);
        return data.schemas;
    }

    public int asyncQueryStart(String query) throws IOException {
        AsyncQueryRequest request = new AsyncQueryRequest();
        request.query = query;
        String json = mapper.writeValueAsString(request);
        HttpResponse response = doJsonPost("/async-query-start", json);
        checkQueryError(response);
        AsyncQueryResponse data = getObjFromResponse(response, AsyncQueryResponse.class);
        return data.queryId;
    }

    public void checkQueryError(HttpResponse response) throws IOException {
        int code = response.getStatusLine().getStatusCode();
        if (code == BAD_REQUEST) {
            QueryErrorResponse errorResponse = getObjFromResponse(response, QueryErrorResponse.class);
            StringBuilder msg = new StringBuilder(errorResponse.errorType + ": ");
            Map error = errorResponse.error;
            if (error.containsKey("prettyMessage")) {
                msg.append(error.get("prettyMessage"));
            } else if (error.containsKey("errors")) {
                for (Object e : (ArrayList<Object>) error.get("errors")) {
                    LinkedHashMap map = (LinkedHashMap<String, Object>) e;
                    msg.append("\n\t");
                    msg.append(map.get("errorType") + ": ");
                    msg.append(map.get("prettyMessage"));
                }
            } else {
                msg.append(getJsonContent(response));
            }
            throw new RawClientException(msg.toString());
        } else if (code == INTERNAL_SERVER_ERROR) {
            GenericErrorResponse error = getObjFromResponse(response, GenericErrorResponse.class);
            throw new RawClientException(error.exceptionType + ": " + error.message);
        } else if (code != HTTP_OK) {
            throw new RawClientException("async-query-start request failed with code " + code +
                    " response: " +  EntityUtils.toString(response.getEntity())
            );
        }
    }

    AsyncQueryNextResponse asyncQueryNext(AsyncQueryNextRequest request) throws IOException {
        AsyncQueryNextResponse data = doJsonPost("/async-query-next", request, AsyncQueryNextResponse.class);
        data.queryId = request.queryId;
        return data;
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
        AsyncQueryCloseResponse data = doJsonPost("/async-query-close", request, AsyncQueryCloseResponse.class);
    }

    public AsyncQueryNextResponse pollQuery(String query, int numberOfResults) throws IOException {
        int queryId = asyncQueryStart(query);
        AsyncQueryNextRequest request = new AsyncQueryNextRequest();
        request.queryId = queryId;
        request.numberResults = numberOfResults;
        logger.fine("polling queryId: " + queryId);
        while (true) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RawClientException("sleep interruped while polling " + e.getMessage());
            }
            AsyncQueryNextResponse data = asynQueryNext(request);
            logger.fine("polling response, size: " + data.size + " hasMore: " + data.hasMore);
            if (data.size > 0) {
                logger.fine("got results: compilation time: " + data.compilationTime +
                        " execution time: " + data.executionTime);
                return data;
            }
        }
    }

    AsyncQueryNextResponse asynQueryNext(AsyncQueryNextRequest request) throws IOException {
        AsyncQueryNextResponse data = doJsonPost("/async-query-next", request, AsyncQueryNextResponse.class);
        data.queryId = request.queryId;
        return data;
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

    private <T> T getObjFromResponse(HttpResponse response, Class<T> tClass) throws IOException {
        String json = getJsonContent(response);
        T obj = mapper.readValue(json, tClass);
        return obj;
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
            throw new RawClientException(path + " request failed with code " + code +
                    " content: " + content);
        }
        return getObjFromResponse(response, responseClass);
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
        return mapper.readValue(json, tClass);
    }

}