package raw.jdbc.oauth2;

import raw.jdbc.oauth2.OAuthConstants;

public class PasswdCredentials {

    private String clientId;
    private String clientSecret;
    private String username;
    private String password;

    public PasswdCredentials(
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
}
