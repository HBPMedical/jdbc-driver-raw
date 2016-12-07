package raw.jdbc;

import java.sql.SQLException;

/**
 * Created by torcato on 07.12.16.
 */
public class ArrayRsMetada extends RsMetaData {


    ArrayRsMetada(Object[][] data, String[] names) throws SQLException {
        super(null, null);
        this.columnNames = names;
        types = new int[columnNames.length];
        for(int i = 0; i < columnNames.length ; i ++ ){
            types[i] = objToType(data[0][i]);
        }

    }
}
