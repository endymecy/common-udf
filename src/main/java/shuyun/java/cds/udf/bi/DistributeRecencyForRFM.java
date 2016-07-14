package shuyun.java.cds.udf.bi;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Created by endy on 2015/10/12.
 */
@Description(
        name = "distribute_recency_for_rfm",
        value = "_FUNC_(arg1)"
)
public class DistributeRecencyForRFM extends UDF{
    public Integer evaluate(Integer freq) {
        return freq.intValue() <= 30?new Integer(1):(freq.intValue() <= 90?new Integer(2):(freq.intValue() <= 180?new Integer(3):(freq.intValue() <= 360?new Integer(4):new Integer(5))));
    }
}
