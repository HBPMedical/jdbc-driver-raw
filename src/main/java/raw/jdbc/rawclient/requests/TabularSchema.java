package raw.jdbc.rawclient.requests;

public class TabularSchema {
    class Column {
        public String name;
        public String type;
    }

    public String name;
    public String sql;
    public Column[] schemas;

}

