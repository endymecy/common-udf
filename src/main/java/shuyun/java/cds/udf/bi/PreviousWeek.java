package shuyun.java.cds.udf.bi;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.GregorianCalendar;

/**
 * Created by endy on 2015/10/10.
 * 返回上一个星期
 */
@Description(
        name = "previous_week",
        value = "_FUNC_(year,week) -previous week of given date"
)
public class PreviousWeek extends UDF{
    public String evaluate(Integer year,Integer week) {
        if(year == null || week == null) {
            return null;
        } else {

            int previousWeek = week - 1;
            if(previousWeek == 0) {
                --year;
                previousWeek = this.getWeeks(year);
            }
            return year + "." + previousWeek;
        }
    }

    public int getWeeks(int year) {
        GregorianCalendar dc = new GregorianCalendar();
        Date d = null;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        String baseDate = year + "1231";

        try {
            d = sdf.parse(baseDate);
            dc.setTime(d);
            dc.set(7, 2);
            double e = (double)dc.get(6);
            System.out.println((int)Math.ceil(e / 7.0D));
            return (int)Math.ceil(e / 7.0D);
        } catch (ParseException var8) {
            var8.printStackTrace();
            return 0;
        }
    }

}
