package raw.jdbc.rawclient.requests;


public class QueryBlockResponse {
    public int executionTime;
    public Object[] data;
    public int size;
    public int compilationTime;
    public int start;
    public boolean hasMore;
    public String token;
}

