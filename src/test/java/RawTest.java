import junit.framework.TestCase;
import raw.jdbc.rawclient.requests.PasswordTokenRequest;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.logging.LogManager;
import java.util.logging.Logger;

public class RawTest {

    Properties conf;
    static Logger logger = Logger.getLogger(TestDriver.class.getName());
    PasswordTokenRequest credentials;
    String authServer;

    public RawTest() {
        try {
            conf = new Properties();
            conf.load(getInputStream("test.properties"));

            LogManager.getLogManager()
                    .readConfiguration(getInputStream("test.properties"));

            credentials = new PasswordTokenRequest();
            credentials.client_id = "raw-jdbc";
            credentials.grant_type = "password";
            credentials.username = conf.getProperty("user");
            credentials.password = conf.getProperty("password");
            authServer = conf.getProperty("auth_server");
            if (authServer == null) {
                authServer = "http://localhost:9000/oauth2/access_token";
                logger.warning("auth server property not found using default value: http://localhost:9000/oauth2/access_token");
            }

        } catch (IOException e){
            logger.severe("could not load configuration: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private InputStream getInputStream(String path) {
        return RawTest.class.getClassLoader().getResourceAsStream(path);
    }
}
