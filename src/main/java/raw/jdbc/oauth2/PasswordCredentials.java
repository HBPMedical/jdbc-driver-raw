package raw.jdbc.oauth2;

import java.util.Properties;

public class PasswordCredentials {

    private String clientId;
    private String clientSecret;
    private String username;
    private String password;

    public PasswordCredentials(
            String clientId,
            String clientSecret,
            String username,
            String password) {

        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.username = username;
        this.password = password;
    }

    public String getGrantType() {
        return OAuthConstants.PASSWORD;
    }

    public String getClientId() {
        return clientId;
    }

    public String getClientSecret() {
        return clientSecret;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public static PasswordCredentials fromProperties(Properties config) {
        PasswordCredentials oauthDetails = new PasswordCredentials(
                config.getProperty(OAuthConstants.CLIENT_ID),
                config.getProperty(OAuthConstants.CLIENT_SECRET),
                config.getProperty(OAuthConstants.USERNAME),
                config.getProperty(OAuthConstants.PASSWORD)
        );

        return oauthDetails;
    }
}
