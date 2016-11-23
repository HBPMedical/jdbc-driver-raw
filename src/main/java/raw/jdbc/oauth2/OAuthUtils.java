package raw.jdbc.oauth2;

import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;
import org.json.simple.parser.JSONParser;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;


public class OAuthUtils {

    public static final String ACCESS_TOKEN = "access_token";
    public static final String TOKEN_TYPE = "token_type";
    public static final String EXPIRES_IN = "expires_in";
    public static final String REFRESH_TOKEN = "refresh_token";
    public static final String CLIENT_ID = "client_id";
    public static final String CLIENT_SECRET = "client_secret";
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";
    public static final String GRANT_TYPE = "grant_type";
    public static final String JSON_CONTENT = "application/json";
    public static final String XML_CONTENT = "application/xml";
    public static final String URL_ENCODED_CONTENT = "application/x-www-form-urlencoded";

    public static final int HTTP_OK = 200;
    public static final int HTTP_FORBIDDEN = 403;
    public static final int HTTP_UNAUTHORIZED = 401;

    /**Gets a token using password authentication to an oauth2 server
     * @param credentials
     * @return token
     * @throws IOException
     */
    public static TokenResponse getPasswdGrantToken(String authUrl, PasswordCredentials credentials) throws IOException {

        HttpPost post = new HttpPost(authUrl);

        List<BasicNameValuePair> parametersBody = new ArrayList<BasicNameValuePair>();
        parametersBody.add(new BasicNameValuePair(GRANT_TYPE, credentials.getGrantType()));
        parametersBody.add(new BasicNameValuePair(CLIENT_ID, credentials.getClientId()));
        parametersBody.add(new BasicNameValuePair(CLIENT_SECRET, credentials.getClientSecret()));
        parametersBody.add(new BasicNameValuePair(USERNAME, credentials.getUsername()));
        parametersBody.add(new BasicNameValuePair(PASSWORD, credentials.getPassword()));


        HttpClient client = new DefaultHttpClient();
        HttpResponse response;
        post.setEntity(new UrlEncodedFormEntity(parametersBody));

        response = client.execute(post);
        int code = response.getStatusLine().getStatusCode();
        if (code != HTTP_OK) {
            throw new IOException("HTTP request using clientId, secret for token failed with code " + code);
        }
        Map<String, Object> map = handleResponse(response);

        TokenResponse oauthToken = new TokenResponse();
        oauthToken.acessToken = (String) map.get(ACCESS_TOKEN);
        oauthToken.expiresIn = (java.lang.Long) map.get(EXPIRES_IN);
        oauthToken.refreshToken = (String) map.get(REFRESH_TOKEN);
        oauthToken.tokenType = (String) map.get(TOKEN_TYPE);
        return  oauthToken;
    }

    static Map handleResponse(HttpResponse response) {
        String contentType = JSON_CONTENT;
        if (response.getEntity().getContentType() != null) {
            contentType = response.getEntity().getContentType().getValue();
        }
        if (contentType.contains(JSON_CONTENT)) {
            return handleJsonResponse(response);
        } else if (contentType.contains(URL_ENCODED_CONTENT)) {
            return handleURLEncodedResponse(response);
        } else if (contentType.contains(XML_CONTENT)) {
            return handleXMLResponse(response);
        } else {
            // Unsupported Content type
            throw new RuntimeException(
                    "Cannot handle "
                            + contentType
                            + " content type. Supported content types include JSON, XML and URLEncoded");
        }
    }

    private static Map handleJsonResponse(HttpResponse response) {
        Map<String, Object> oauthLoginResponse;
        try {
            String json = EntityUtils.toString(response.getEntity());
            oauthLoginResponse = (Map<String, Object>) new JSONParser().parse(json);
            //TODO: Throw a more specific exception
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        return oauthLoginResponse;
    }

    private static Map handleURLEncodedResponse(HttpResponse response) {
        Map<String, String> oauthResponse = new HashMap<String, String>();
        HttpEntity entity = response.getEntity();

        try {
            //TODO: make this work with other encodings
            List<NameValuePair> list = URLEncodedUtils.parse(
                    EntityUtils.toString(entity), Charset.forName(HTTP.UTF_8));
            for (NameValuePair pair : list) {
                oauthResponse.put(pair.getName(), pair.getValue());
            }

        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        return oauthResponse;
    }

    private static Map handleXMLResponse(HttpResponse response) {
        Map<String, Object> oauthResponse = new HashMap<String, Object>();
        try {
            String xmlString = EntityUtils.toString(response.getEntity());
            DocumentBuilderFactory factory = DocumentBuilderFactory
                    .newInstance();
            DocumentBuilder db = factory.newDocumentBuilder();
            InputSource inStream = new InputSource();
            inStream.setCharacterStream(new StringReader(xmlString));
            Document doc = db.parse(inStream);
            parseXMLDoc(null, doc, oauthResponse);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        return oauthResponse;
    }

    private static void parseXMLDoc(Element element, Document doc,
                                   Map<String, Object> oauthResponse) {
        NodeList child;
        if (element == null) {
            child = doc.getChildNodes();

        } else {
            child = element.getChildNodes();
        }
        for (int j = 0; j < child.getLength(); j++) {
            if (child.item(j).getNodeType() == org.w3c.dom.Node.ELEMENT_NODE) {
                org.w3c.dom.Element childElement = (org.w3c.dom.Element) child
                        .item(j);
                if (childElement.hasChildNodes()) {
                    oauthResponse.put(childElement.getTagName(),
                            childElement.getTextContent());
                    parseXMLDoc(childElement, null, oauthResponse);
                }
            }
        }
    }
}