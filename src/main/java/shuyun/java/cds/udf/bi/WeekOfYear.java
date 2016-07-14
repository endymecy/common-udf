package shuyun.java.cds.udf.bi;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

/**
 * Created by endy on 2015/10/10.
 */

@Description(
        name = "week_of_year",
        value = "_FUNC_(year,month,day) -the week of week year"
)
public class WeekOfYear extends UDF{
    private Calendar dc = new GregorianCalendar();
    private Date d = null;
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");

    public String evaluate(Integer year,Integer month,Integer day) {
        if(year==null || month==null || day==null)
            return null;
        try {
            this.d = this.sdf.parse(new Date(year,month,day).toString());
            this.dc.setTime(this.d);
            this.dc.set(7, 2);
            String e = String.valueOf(this.dc.get(1));
            double days = (double)this.dc.get(6);
            String weekth = String.valueOf((int)Math.ceil(days / 7.0D));
            return e + "." + weekth;
        } catch (ParseException var6) {
            var6.printStackTrace();
            return null;
        }
    }
}
