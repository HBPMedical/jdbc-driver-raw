package raw.jdbc.rawclient.requests;

import java.util.ArrayList;

public class AsyncQueryNextResponse {
    public ArrayList data;
    public int start;
    public int size;
    public boolean hasMore;
    public boolean reachedMaxResults;
    public long compilationTime;
    public long executionTime;
    public int queryId; // this is not part of the response from the server, we fill it up the in client
}