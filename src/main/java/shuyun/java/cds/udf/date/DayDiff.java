package shuyun.java.cds.udf.date;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * Created by endy on 2015/10/10.
 * 返回两个日期之间的差值
 *day_diff( "20151001", "20151002") == 1
 */
@Description(
        name = "day_diff",
        value = "_FUNC_(a,b) - The difference in days of two YYYYMMDD dates"
)
public class DayDiff extends UDF{
    private static final DateTimeFormatter YYYYMMDD = DateTimeFormat.forPattern("YYYYMMdd");

    public Integer evaluate(String date1Str, String date2Str) {
        DateTime dt1 = YYYYMMDD.parseDateTime(date1Str);
        DateTime dt2 = YYYYMMDD.parseDateTime(date2Str);
        int dayDiff = Days.daysBetween(dt1, dt2).getDays();

        return dayDiff;
    }
}
