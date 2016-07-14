package shuyun.java.cds.udf.date;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * Created by endy on 2015/10/10.
 *
 * 返回当前日期增加后的日期，为YYYYMMDD
 */

@Description(name = "add_days",
        value = "_FUNC_(day,addNum) - Return the added date for a day.\n")
public class AddDays extends UDF {

    private static final DateTimeFormatter YYYYMMDD = DateTimeFormat.forPattern("YYYYMMdd");

    public String evaluate(String dateStr, Integer numDays) {
        if(dateStr==null || numDays ==null)
            return null;
        DateTime dt = YYYYMMDD.parseDateTime(dateStr);
        DateTime addedDt = dt.plusDays(numDays);
        String addedDtStr = YYYYMMDD.print(addedDt);
        return addedDtStr;
    }
}
