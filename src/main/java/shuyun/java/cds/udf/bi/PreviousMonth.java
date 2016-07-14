package shuyun.java.cds.udf.bi;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;

/**
 * Created by endy on 2015/10/10.
 * 返回给定时间的前一个月
 */

@Description(
        name = "previous_month",
        value = "_FUNC_(year,month) -previous month of given date"
)
public class PreviousMonth extends UDF{
    public String evaluate(Integer year, Integer month) {
        if (year == null || month == null || month < 1 || month > 12 ) {
            return null;
        }else {
            int previous_month = month - 1;
            if(previous_month == 0) {
                --year;
                previous_month = 12;
            }

            return String.valueOf(year) + previous_month;
        }
    }
}
