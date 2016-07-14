package shuyun.java.cds.udf.timeserials;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by endy on 2015/10/13.
 * 返回移动窗口中的值的标准差
 */
@Description(
        name = "moving_stdev",
        value = " return the moving standard deviation of a time series for a given timewindow"
)
public class MovingStdev extends GenericUDF{

    private ListObjectInspector listInspector;
    private IntObjectInspector dayWindowInspector;

    private List<Double> parseDoubleList(List<Object> objList) {
        List<Double> arrList = new ArrayList<Double>();
        for (Object obj : objList) {

            Object dblObj = ((PrimitiveObjectInspector) (listInspector.getListElementObjectInspector())).getPrimitiveJavaObject(obj);
            if (dblObj instanceof Number) {
                Number dblNum = (Number) dblObj;
                arrList.add(dblNum.doubleValue());
            } else {
                //// Try to coerce it otherwise
                String dblStr = (dblObj.toString());
                try {
                    Double dblCoerce = Double.parseDouble(dblStr);
                    arrList.add(dblCoerce);
                } catch (NumberFormatException formatExc) {
                }
            }

        }
        return arrList;
    }

    public List<Double> evaluate(List<Object> timeSeriesObj, int dayWindow) {

        List<Double> timeSeries = this.parseDoubleList(timeSeriesObj);

        List<Double> mvnStdevTimeSeries = new ArrayList<Double>(timeSeries.size());
        double mvnTotal = 0.0;
        double mvnSqTotal = 0.0;

        for (int i = 0; i < timeSeries.size(); ++i) {
            mvnTotal += timeSeries.get(i); //// Do we want to set days befo
            mvnSqTotal += Math.pow(timeSeries.get(i), 2);
            if (i >= dayWindow) {
                mvnTotal -= timeSeries.get(i - dayWindow);
                mvnSqTotal -= Math.pow(timeSeries.get(i - dayWindow), 2);
                double mvnStdev = Math.sqrt(mvnSqTotal / (double) dayWindow - Math.pow(mvnTotal / (double) dayWindow, 2));
                mvnStdevTimeSeries.add(mvnStdev);
            } else {
                if (i > 0) {  ///smooth out according to number of days for less than day window
                    double mvnStdev = Math.sqrt(mvnSqTotal / (double) (i + 1.0) - Math.pow(mvnTotal / (double) (i + 1.0), 2));
                    mvnStdevTimeSeries.add(mvnStdev);
                } else {
                    mvnStdevTimeSeries.add(0.0); ///
                }
            }
        }
        return mvnStdevTimeSeries;
    }

    @Override
    public Object evaluate(DeferredObject[] arg0) throws HiveException {
        List argList = listInspector.getList(arg0[0].get());
        int dayWindow = dayWindowInspector.get(arg0[1].get());
        if (argList != null)
            return evaluate(argList, dayWindow);
        else
            return null;
    }

    @Override
    public String getDisplayString(String[] arg0) {
        return "moving_stdev(" + arg0[0] + ")";
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arg0)
            throws UDFArgumentException {
        if (arg0.length != 1 && arg0.length != 2) {
            throw new UDFArgumentException(" MovingStdevUDF takes 2 arguments, array<string>, string");
        }


        this.listInspector = (ListObjectInspector) arg0[0];

        if (this.listInspector.getCategory() != ObjectInspector.Category.LIST) {
            throw new UDFArgumentException(" MovingStdevUDF takes an array as first argument");
        }
        this.dayWindowInspector = (IntObjectInspector) arg0[1];

        if (this.dayWindowInspector.getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentException(" MovingStdevUDF takes a numeric string as second argument");
        }

        return ObjectInspectorFactory.getStandardListObjectInspector(
                PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
    }
}
