package raw.jdbc;

import java.io.IOException;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import raw.jdbc.oauth2.PasswordCredentials;
import raw.jdbc.oauth2.TokenResponse;

public class RawRestClient {

    private static final String JSON_CONTENT = "application/json";
    private static final int HTTP_OK = 200;

    private static Logger logger = Logger.getLogger(RawRestClient.class.getName());
    private static JSONParser jsonParser = new JSONParser();

    private HttpClient client = HttpClientBuilder.create().build();
    private String executerUrl;
    private TokenResponse credentials;

    public RawRestClient(String executor, TokenResponse token) {
        this.credentials = token;
        this.executerUrl = executor;
    }

    /**
     * Gets a token using password authentication to an oauth2 server
     *
     * @param credentials
     * @return Return token response
     * @throws IOException
     */
    public static TokenResponse getPasswdGrantToken(String authUrl, PasswordCredentials credentials) throws IOException {
        HttpClient client = HttpClientBuilder.create().build();
        HttpPost post = new HttpPost(authUrl);

        JSONObject json = new JSONObject();
        json.put("grant_type", credentials.getGrantType());
        json.put("client_id", credentials.getClientId());
        json.put("username", credentials.getUsername());
        json.put("password", credentials.getPassword());
        StringEntity entity = new StringEntity(json.toString(), "UTF-8");
        entity.setContentType("application/json");
        post.setEntity(entity);

        HttpResponse response = client.execute(post);
        int code = response.getStatusLine().getStatusCode();
        if (code != HTTP_OK) {
            throw new IOException("HTTP request using clientId, secret for token failed with code " + code);
        }

        Map map = getMapFromJsonResponse(response);

        TokenResponse oauthToken = new TokenResponse();
        oauthToken.accessToken = (String) map.get("access_token");
        oauthToken.expiresIn = (java.lang.Long) map.get("expires_in");
        oauthToken.refreshToken = (String) map.get("refresh_token");
        oauthToken.tokenType = (String) map.get("token_type");
        return oauthToken;
    }

    public String getVersion() throws IOException, ParseException {
        HttpResponse response = doGet("/version");
        String contentType = response.getEntity().getContentType().getValue();
        assert (contentType.contains("text/plain"));
        String data = EntityUtils.toString(response.getEntity());
        return data;
    }

    public String[] getSchemas() throws IOException, ParseException {
        Map data = getMapFromJsonResponse(doGet("/schemas"));
        JSONArray schemas = (JSONArray) data.get("schemas");
        return (String[]) schemas.toArray(new String[]{});
    }

    private static Map getMapFromJsonResponse(HttpResponse response) throws IOException {

        String contentType = response.getEntity().getContentType().getValue();
        if (!contentType.contains(JSON_CONTENT)) {
            throw new RuntimeException(
                    "Expected content with '" + JSON_CONTENT + "' but got instead " + contentType);
        }
        String data = EntityUtils.toString(response.getEntity());
        try {
            Map map = (Map<String, Object>) jsonParser.parse(data);
            return map;
        } catch (ParseException e) {
            throw new IOException("Unable to parse json response from request" + data);
        }
    }

    public Long asyncQueryStart(String query) throws IOException {
        JSONObject json = new JSONObject();
        json.put("query", query);
        HttpResponse response = doJsonPost("/async-query-start", json);
        Map data = getMapFromJsonResponse(response);
        logger.fine("reponse data: " + data);
        return (Long) data.get("queryId");
    }

    public Map queryStart(String query, int resultsPerPage) throws IOException {
        JSONObject json = new JSONObject();
        json.put("query", query);
        json.put("resultsPerPage", resultsPerPage);
        HttpResponse response = doJsonPost("/query-start", json);
        int code = response.getStatusLine().getStatusCode();
        if (code != 200) {
            throw new RuntimeException("Error starting query");
        }
        Map data = getMapFromJsonResponse(response);
        return data;
    }

    private HttpResponse doJsonPost(String path, JSONObject data) throws IOException {
        HttpPost post = new HttpPost(executerUrl + path);
        post.setHeader("Authorization", "Bearer " + credentials.accessToken);
        StringEntity entity = new StringEntity(data.toString(), "UTF-8");
        entity.setContentType("application/json");
        post.setEntity(entity);
        HttpResponse response = client.execute(post);
        int code = response.getStatusLine().getStatusCode();
        logger.fine("Finished get to path: '" + path + "' with code: " + code);
        return response;
    }

    private HttpResponse doGet(String path) throws IOException {
        String url = executerUrl + path;
        logger.fine("sending get to " + url);
        HttpGet get = new HttpGet(url);
        get.setHeader("Authorization", "Bearer " + credentials.accessToken);
        HttpResponse response = client.execute(get);
        int code = response.getStatusLine().getStatusCode();
        logger.fine("Finished get to path: " + path + "with code: " + code);
        return response;
    }

}