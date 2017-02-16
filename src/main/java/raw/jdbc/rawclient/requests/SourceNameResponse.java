package raw.jdbc.rawclient.requests;

import java.util.LinkedHashMap;

public class SourceNameResponse {
    public String name;
    public SourceLocation location;
    public LinkedHashMap<String, Object> locationStatus;
    public LinkedHashMap<String, Object> tipe;
}

