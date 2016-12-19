package raw.jdbc.rawclient.requests;


import java.util.LinkedHashMap;

public class QueryErrorResponse {
    public String errorType;
    public LinkedHashMap<String, Object> error;
}
