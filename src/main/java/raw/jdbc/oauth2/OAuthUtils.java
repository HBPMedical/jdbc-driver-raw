package raw.jdbc.oauth2;

import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
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
    public static PasswdCredentials createCredentials(Properties config) {
        PasswdCredentials oauthDetails = new PasswdCredentials(
                config.getProperty(OAuthConstants.CLIENT_ID),
                config.getProperty(OAuthConstants.CLIENT_SECRET),
                config.getProperty(OAuthConstants.USERNAME),
                config.getProperty(OAuthConstants.PASSWORD)
        );

        return oauthDetails;
    }

    /**
     * @param credentials
     * @return token
     * @throws IOException
     */
    public static TokenResponse getPasswdGrantToken(String authUrl, PasswdCredentials credentials) throws IOException {

        HttpPost post = new HttpPost(authUrl);

        List<BasicNameValuePair> parametersBody = new ArrayList<BasicNameValuePair>();
        parametersBody.add(new BasicNameValuePair(OAuthConstants.GRANT_TYPE,
                credentials.getGrantType()));
        parametersBody.add(new BasicNameValuePair(OAuthConstants.CLIENT_ID,
                credentials.getClientId()));
        parametersBody.add(new BasicNameValuePair(
                OAuthConstants.CLIENT_SECRET, credentials.getClientSecret()));
        parametersBody.add(new BasicNameValuePair(OAuthConstants.USERNAME,
                credentials.getUsername()));
        parametersBody.add(new BasicNameValuePair(OAuthConstants.PASSWORD,
                credentials.getPassword()));


        HttpClient client = new DefaultHttpClient();
        HttpResponse response;
        post.setEntity(new UrlEncodedFormEntity(parametersBody));

        response = client.execute(post);
        int code = response.getStatusLine().getStatusCode();
        if (code != OAuthConstants.HTTP_OK) {
            throw new IOException("HTTP request using clientId, secret for token failed with code " + code);
        }
        Map<String, Object> map = handleResponse(response);

        TokenResponse oauthToken = new TokenResponse();
        oauthToken.acessToken = (String) map.get(OAuthConstants.ACCESS_TOKEN);
        oauthToken.expiresIn = (java.lang.Long) map.get(OAuthConstants.EXPIRES_IN);
        oauthToken.refreshToken = (String) map.get(OAuthConstants.REFRESH_TOKEN);
        oauthToken.tokenType = (String) map.get(OAuthConstants.TOKEN_TYPE);
        return  oauthToken;
    }

    static Map handleResponse(HttpResponse response) {
        String contentType = OAuthConstants.JSON_CONTENT;
        if (response.getEntity().getContentType() != null) {
            contentType = response.getEntity().getContentType().getValue();
        }
        if (contentType.contains(OAuthConstants.JSON_CONTENT)) {
            return handleJsonResponse(response);
        } else if (contentType.contains(OAuthConstants.URL_ENCODED_CONTENT)) {
            return handleURLEncodedResponse(response);
        } else if (contentType.contains(OAuthConstants.XML_CONTENT)) {
            return handleXMLResponse(response);
        } else {
            // Unsupported Content type
            throw new RuntimeException(
                    "Cannot handle "
                            + contentType
                            + " content type. Supported content types include JSON, XML and URLEncoded");
        }

    }

    static Map handleJsonResponse(HttpResponse response) {
        Map<String, Object> oauthLoginResponse;
        try {
            String json = EntityUtils.toString(response.getEntity());
            System.out.println("token response: " + json);
            oauthLoginResponse = (Map<String, Object>) new JSONParser().parse(json);
            //TODO: Throw a more specific exception
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        return oauthLoginResponse;
    }

    static Map handleURLEncodedResponse(HttpResponse response) {
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

    static Map handleXMLResponse(HttpResponse response) {
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

    static void parseXMLDoc(Element element, Document doc,
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
