import org.junit.Test;
import raw.jdbc.RawDriver;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;

public class TestConnection {
    @Test
    public void testGetToken() throws SQLException {

        Properties info = new Properties();
        info.setProperty("oAuthUri", "http://localhost:9000/oauth2/authenticate");
        info.setProperty("userId", "userId");



    }
}
