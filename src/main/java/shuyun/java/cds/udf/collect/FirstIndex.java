package shuyun.java.cds.udf.collect;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

/**
 * Created by endy on 2015/10/12.
 * 返回数组的第一个元素
 */
@Description(name = "first_index",
        value = "_FUNC_(x) - First value in an array "
)
public class FirstIndex extends GenericUDF {
    private ListObjectInspector listInspector;
    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length != 1) {
            throw new UDFArgumentException("first_index takes an array as an argument.");
        }
        if (objectInspectors[0].getCategory() != ObjectInspector.Category.LIST) {
            throw new UDFArgumentException("first_index takes an array as an argument.");
        }
        listInspector = (ListObjectInspector) objectInspectors[0];

        return listInspector.getListElementObjectInspector();
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        Object list = deferredObjects[0].get();
        if (listInspector.getListLength(list) > 0) {
            Object unInsp = listInspector.getListElement(list, 0);
            return unInsp;
        } else {
            return null;
        }
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "first_index( " + strings[0] + " )";
    }
}
