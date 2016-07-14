package shuyun.java.cds.udf.sketch;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Created by endy on 2015/10/13.
 */
@Description(name = "md5",
        value = "_FUNC_(s) - Returns md5 text from given text"
)
public class Md5 extends UDF {
    public Text evaluate(final Text s) {
        if (s == null) {
            return null;
        }
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(s.toString().getBytes());
            byte[] md5hash = md.digest();
            StringBuilder builder = new StringBuilder();
            for (byte b : md5hash) {
                builder.append(Integer.toString((b & 0xff) + 0x100, 16).substring(1));
            }
            return new Text(builder.toString());
        } catch (NoSuchAlgorithmException nsae) {
            System.out.println("Cannot find digest algorithm");
            System.exit(1);
        }
        return null;
    }
}
