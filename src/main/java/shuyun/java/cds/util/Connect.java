package shuyun.java.cds.util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

/**
 * Created by luping.qiu on 2015/9/25.
 */
public class Connect {

    public Connection getConnection() {
        InputStream inputStream = null;
        Connection conn = null;
        try {
            Properties prop = new Properties();
            String propFileName = "db.properties";

            inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);
            if (inputStream != null) {
                prop.load(inputStream);
                Class.forName(prop.getProperty("jdbc.driverClassName"));
                conn = DriverManager.getConnection(prop.getProperty("jdbc.url") + prop.getProperty("jdbc.db"), prop.getProperty("jdbc.username"), prop.getProperty("jdbc.password"));
                inputStream.close();
                return conn;
            } else {
                throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
            }
        }catch(Exception e){
            e.printStackTrace();
            throw new RuntimeException("mysql connect exception");
        }
    }

    public static void main(String[] args){
        Connect con = new Connect();
        con.getConnection();
    }
}
