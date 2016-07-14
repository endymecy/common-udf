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
 * 返回移动时间窗口中的值的平均值
 */
@Description(
        name = "moving_avg",
        value = " return the moving average of a time series for a given timewindow"
)
public class MovingAvg extends GenericUDF {
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

        List<Double> mvnAvgTimeSeries = new ArrayList<Double>(timeSeries.size());
        double mvnTotal = 0.0;

        for (int i = 0; i < timeSeries.size(); ++i) {
            mvnTotal += timeSeries.get(i); //// Do we want to set days befo
            if (i >= dayWindow) {
                mvnTotal -= timeSeries.get(i - dayWindow);
                double mvnAvg = mvnTotal / ((double) dayWindow);
                mvnAvgTimeSeries.add(mvnAvg);
            } else {
                if (i > 0) {  ///smooth out according to number of days for less than day window
                    double mvnAvg = mvnTotal / ((double) i + 1.0);
                    mvnAvgTimeSeries.add(mvnAvg);
                } else {
                    mvnAvgTimeSeries.add(mvnTotal); ///
                }
            }
        }
        return mvnAvgTimeSeries;
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        this.listInspector = (ListObjectInspector) objectInspectors[0];
        this.dayWindowInspector = (IntObjectInspector) objectInspectors[1];

        return ObjectInspectorFactory.getStandardListObjectInspector(
                PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        List argList = listInspector.getList(deferredObjects[0].get());
        int dayWindow = dayWindowInspector.get(deferredObjects[1].get());
        if (argList != null)
            return evaluate(argList, dayWindow);
        else
            return null;
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "moving_avg(" + strings[0] + ")";
    }

}
