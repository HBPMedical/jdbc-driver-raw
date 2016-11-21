package oauth;

public class OAuth2Details {

    private String scope;
    private String grantType;
    private String clientId;
    private String clientSecret;
    private String accessToken;
    private String refreshToken;
    private String username;
    private String password;
    private String authenticationServerUrl;

    public OAuth2Details(
            String authenticationServerUrl,
            String scope,
            String grantType,
            String clientId,
            String clientSecret,
            String username,
            String password) {

        this.scope = scope;
        this.grantType = grantType;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.username = username;
        this.password = password;
        this.authenticationServerUrl = authenticationServerUrl;
    }

    public String getScope() {
        return scope;
    }

    public String getGrantType() {
        return grantType;
    }

    public String getClientId() {
        return clientId;
    }

    public String getClientSecret() {
        return clientSecret;
    }

    public String getAuthenticationServerUrl() {
        return authenticationServerUrl;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getAccessToken() {
        return accessToken;
    }

    public void setAccessToken(String accessToken) {
        this.accessToken = accessToken;
    }

    public String getRefreshToken() {
        return refreshToken;
    }

    public void setRefreshToken(String refreshToken) {
        this.refreshToken = refreshToken;
    }

}
