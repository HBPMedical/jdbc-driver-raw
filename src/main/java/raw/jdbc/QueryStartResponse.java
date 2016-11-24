package raw.jdbc;

/**
 * Created by cesar on 24.11.16.
 */
public class QueryStartResponse {
    int executionTime;
    Object[] data;
    int size;
    int compilationTime;
    int start;
    boolean hasMore;
    String token;
}
