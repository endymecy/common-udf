package shuyun.java.cds.udf.date;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;

/**
 * Created by endy on 2015/9/24.
 *
 * 返回给定datestamp的星期值
 * sunday的值为0，Null的返回值为Null
 */

@Description(name = "day_of_week",
        value = "_FUNC_(year, month, day) - Find the weekday of the date.\n")
public class DayofWeek extends UDF{
    public Integer evaluate(Integer year, Integer month, Integer day) throws UDFArgumentException {
        if (year == null || month == null || month < 1 || month > 12 || day == null || day < 1 || day > 31) {
            throw new UDFArgumentException("Error: Year-Month-Day outside of valid range!\n");
        }
        //在hive中January=1，所以要修正减1
        Calendar date = new GregorianCalendar(year, month-1, day);
        //一般情况下，Sunday = 0，所以在此修正减1
        return date.get(Calendar.DAY_OF_WEEK) - 1;
    }
    public Integer evaluate(ArrayList<String> datearr) throws UDFArgumentException {
        if (datearr.size() != 3) {
            throw new UDFArgumentException("Must provide a size-3 array, containing Year, Month, and Day in order.");
        }
        Integer year = Integer.parseInt(datearr.get(0));
        Integer month = Integer.parseInt(datearr.get(1));
        Integer day = Integer.parseInt(datearr.get(2));
        if (year == null || month == null || month < 1 || month > 12 || day == null || day < 1 || day > 31) {
            throw new UDFArgumentException("Error: Year-Month-Day outside of valid range!\n");
        }
        //在hive中January=1，所以要修正减1
        Calendar date = new GregorianCalendar(year, month-1, day);
        //一般情况下，Sunday = 0，所以在此修正减1
        return date.get(Calendar.DAY_OF_WEEK) - 1;
    }
}
