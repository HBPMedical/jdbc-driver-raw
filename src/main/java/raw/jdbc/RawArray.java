package raw.jdbc;


import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;

/**
 * This is to return array data from a resultset
 * This class is not finished yet and it is just a stump
 * @param <T>
 */
public class RawArray<T> implements Array {
    private ArrayList data;

    RawArray(ArrayList<T> ary) {
        data = ary;
    }

    public String getBaseTypeName() throws SQLException {
        if(data.size() > 0){
            return data.get(0).getClass().getName();
        }
        return null;
    }

    public int getBaseType() throws SQLException {
        return 0;
    }

    public Object getArray() throws SQLException {
        return data.toArray();
    }

    public Object getArray(Map<String, Class<?>> map) throws SQLException {
        return null;
    }

    public Object getArray(long index, int count) throws SQLException {
        return null;
    }

    public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
        return null;
    }

    public ResultSet getResultSet() throws SQLException {
        return null;
    }

    public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
        return null;
    }

    public ResultSet getResultSet(long index, int count) throws SQLException {
        return null;
    }

    public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map) throws SQLException {
        return null;
    }

    public void free() throws SQLException {

    }
}
