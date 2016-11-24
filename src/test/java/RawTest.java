import junit.framework.TestCase;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.logging.LogManager;
import java.util.logging.Logger;

public class RawTest extends TestCase {
    Properties conf;
    static Logger logger = Logger.getLogger(TestDriver.class.getName());

    public RawTest() {
        try {
            conf = new Properties();
            conf.load(getInputStream("test.properties"));

            LogManager.getLogManager()
                    .readConfiguration(getInputStream("test.properties"));
        } catch (IOException e){
            logger.severe("could not load configuration: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private InputStream getInputStream(String path) {
        return RawTest.class.getClassLoader().getResourceAsStream(path);
    }
}
