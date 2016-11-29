package raw.jdbc.rawclient.requests;

public class PasswordTokenRequest {
    public String grant_type;
    public String client_id;
    // this is a optional field of oAuth that we keep always as null;
    public String client_secret;
    public String username;
    public String password;
}


