package shuyun.java.cds.udf.collect;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.List;

/**
 * Created by endy on 2015/10/13.
 * 返回数组的和
 */
@Description(
        name = "sum_array",
        value = " sum an array of doubles"
)
public class SumArray extends GenericUDF{
    private ListObjectInspector listInspector;

    public Double evaluate(List<Object> strArray) {
        double total = 0.0;
        for (Object obj : strArray) {

            Object dblObj = ((PrimitiveObjectInspector) (listInspector.getListElementObjectInspector())).getPrimitiveJavaObject(obj);
            if (dblObj instanceof Number) {
                Number dblNum = (Number) dblObj;
                total += dblNum.doubleValue();
            } else {
                //// Try to coerce it otherwise
                String dblStr = (dblObj.toString());
                try {
                    Double dblCoerce = Double.parseDouble(dblStr);
                    total += dblCoerce;
                } catch (NumberFormatException formatExc) {
                }
            }

        }
        return total;
    }

    @Override
    public Object evaluate(GenericUDF.DeferredObject[] arg0) throws HiveException {
        List argList = listInspector.getList(arg0[0].get());
        if (argList != null)
            return evaluate(argList);
        else
            return null;
    }

    @Override
    public String getDisplayString(String[] arg0) {
        return "sum_array()";
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arg0)
            throws UDFArgumentException {
        this.listInspector = (ListObjectInspector) arg0[0];

        ObjectInspector returnType = PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
        return returnType;
    }
}
