package shuyun.java.cds.udf.bi;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Created by endy on 2015/10/12.
 */
@Description(
        name = "distribute_recency_for_r",
        value = "_FUNC_(arg1,arg2)"
)
public class DistributeRecencyForR extends UDF{
    public Integer evaluate(Integer f, Integer internal) {
        double freq = (double)f.intValue();
        int group;
        if(internal.intValue() == 30) {
            if(freq <= 720.0D) {
                group = Math.max(1, (int)Math.ceil(freq / 30.0D));
                return new Integer(group);
            } else if(freq <= 1080.0D) {
                group = (int)Math.ceil((freq - 720.0D) / 180.0D) + 24;
                return new Integer(group);
            } else {
                return new Integer(27);
            }
        } else if(internal.intValue() == 10) {
            if(freq <= 360.0D) {
                group = Math.max(1, (int)Math.ceil(freq / 10.0D));
                return new Integer(group);
            } else if(freq <= 1080.0D) {
                group = (int)Math.ceil((freq - 360.0D) / 180.0D) + 36;
                return new Integer(group);
            } else {
                return new Integer(41);
            }
        } else {
            return new Integer(0);
        }
    }
}
