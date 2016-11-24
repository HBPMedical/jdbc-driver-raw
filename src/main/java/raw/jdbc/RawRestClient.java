package raw.jdbc;

import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import raw.jdbc.oauth2.PasswordCredentials;
import raw.jdbc.oauth2.TokenResponse;


public class RawRestClient {

    private static final String JSON_CONTENT = "application/json";
    private static final String XML_CONTENT = "application/xml";
    private static final String URL_ENCODED_CONTENT = "application/x-www-form-urlencoded";
    private static final int HTTP_OK = 200;

    private static Logger logger = Logger.getLogger(RawRestClient.class.getName());

    private HttpClient client = HttpClientBuilder.create().build();
    private String executerUrl;
    private TokenResponse credentials;

    public RawRestClient(String executor, TokenResponse token){
        this.credentials = token;
        this.executerUrl = executor;
    }

    /**
     * Gets a token using password authentication to an oauth2 server
     * @param credentials
     * @return Return token response
     * @throws IOException
     */
    public static TokenResponse getPasswdGrantToken(String authUrl, PasswordCredentials credentials) throws IOException {
        HttpClient client = HttpClientBuilder.create().build();
        HttpPost post = new HttpPost(authUrl);

        JSONObject json = new JSONObject();
        json.put( "grant_type", credentials.getGrantType());
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
        String contentType = response.getEntity().getContentType().getValue();
        if (!contentType.contains(JSON_CONTENT)) {
            // Unsupported Content type
            throw new RuntimeException(
                    "Cannot handle " + contentType + " expected " + JSON_CONTENT);
        }

        String data = EntityUtils.toString(response.getEntity());
        Map map;
        try {
            map = (Map<String, Object>) new JSONParser().parse(data);
        } catch (ParseException e) {
            throw new IOException("Unable to parse json response of auth server " + data);
        }

        TokenResponse oauthToken = new TokenResponse();
        oauthToken.acessToken = (String) map.get("access_token");
        oauthToken.expiresIn = (java.lang.Long) map.get("expires_in");
        oauthToken.refreshToken = (String) map.get("refresh_token");
        oauthToken.tokenType = (String) map.get("token_type");
        return oauthToken;
    }

    public String getVersion() throws IOException, ParseException {
        HttpResponse response = doGet("/version");
        String data = EntityUtils.toString(response.getEntity());
        return (String) new JSONParser().parse(data);
    }

    private HttpResponse doJsonPost(String path, JSONObject data) throws IOException {
        HttpPost post = new HttpPost(executerUrl + path);
        post.setHeader("Authorization", "Bearer " + credentials.acessToken);
        StringEntity entity = new StringEntity(data.toString(), "UTF-8");
        entity.setContentType("application/json");
        post.setEntity(entity);
        return client.execute(post);
    }

    private HttpResponse doGet(String path) throws IOException {
        HttpGet get = new HttpGet(executerUrl + path);
        get.setHeader("Authorization", "Bearer " + credentials.acessToken);
        return client.execute(get);
    }

}