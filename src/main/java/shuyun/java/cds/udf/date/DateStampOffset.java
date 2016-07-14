package shuyun.java.cds.udf.date;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;

/**
 * Created by endy on 2015/10/10.
 *
 *  返回给定时间距离夏令时的偏移量。返回秒
 */

@Description(name = "udf_dst_offset",
        value = "_FUNC_(year, month, day) - Return the offset (in seconds) due to daylight savings for a day.\n")
public class DateStampOffset extends UDF {

    public Integer evaluate(Integer year, Integer month, Integer day) {
        if (year == null || month == null || month < 1 || month > 12 || day == null || day < 1 || day > 31) {
            return null;
        }
        Calendar date = new GregorianCalendar(year, month - 1, day);
        return date.get(Calendar.DST_OFFSET) / 1000;
    }

    public Integer evaluate(ArrayList<String> datearr) throws UDFArgumentException {
        if (datearr.size() != 3) {
            throw new UDFArgumentException("Must provide a size-3 array, containing Year, Month, and Day in order.");
        }
        Integer year = Integer.parseInt(datearr.get(0));
        Integer month = Integer.parseInt(datearr.get(1));
        Integer day = Integer.parseInt(datearr.get(2));
        if (year == null || month == null || month < 1 || month > 12 || day == null || day < 1 || day > 31) {
            return null;
        }
        Calendar date = new GregorianCalendar(year, month, day);
        return date.get(Calendar.DST_OFFSET) / 1000;
    }
}
