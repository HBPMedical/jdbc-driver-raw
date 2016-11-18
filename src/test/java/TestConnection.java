import org.junit.Test;
import raw.jdbc.RawDriver;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;

public class TestConnection {
    @Test
    public void testConnection() throws SQLException {
        Driver driver = new RawDriver();
        Properties info = new Properties();

        info.setProperty("oAuthUri", "http://localhost:9000/oauth2/authenticate");
        info.setProperty("redirectUri", "http://localhost:9000/accout/sign_in");
        info.setProperty("userId", "userId");

        Connection conn = driver.connect("localhost:54321", info);
    }
}
