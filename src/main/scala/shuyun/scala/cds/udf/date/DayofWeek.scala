package shuyun.scala.cds.udf.date

import java.util
import java.util.{Calendar, GregorianCalendar}

import org.apache.hadoop.hive.ql.exec.{UDFArgumentException, UDF}

/**
 * Created by endy on 2015/9/24.
 */
class DayofWeek extends UDF{

    @throws(classOf[UDFArgumentException])
    def evaluate(year:Integer,month:Integer,day:Integer): Integer=
    {
      if (year == null || month == null || month < 1 || month > 12 || day == null || day < 1 || day > 31) {
        throw new UDFArgumentException("Error: Year-Month-Day outside of valid range!\n");
      }

      val date: Calendar= new GregorianCalendar(year, month - 1, day);
      date.get(Calendar.DAY_OF_WEEK) - 1
    }

    def evaluate(datearr:util.ArrayList[String]):Integer=
    {
      if (datearr.size != 3) {
        throw new UDFArgumentException("Must provide a size-3 array, containing Year, Month, and Day in order.")
      }
      val year: Integer = datearr.get(0).toInt
      val month: Integer = datearr.get(1).toInt
      val day: Integer = datearr.get(2).toInt

      if (year == null || month == null || month < 1 || month > 12 || day == null || day < 1 || day > 31) {
        throw new UDFArgumentException("Error: Year-Month-Day outside of valid range!\n")
      }

      val date: Calendar= new GregorianCalendar(year, month - 1, day);
      date.get(Calendar.DAY_OF_WEEK) - 1
    }
}
