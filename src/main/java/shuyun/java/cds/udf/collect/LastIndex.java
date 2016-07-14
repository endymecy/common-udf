package shuyun.java.cds.udf.collect;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

/**
 * Created by endy on 2015/10/12.
 */
@Description(name = "last_index",
        value = "_FUNC_(x) - Last value in an array "
)
public class LastIndex extends GenericUDF {
    private ListObjectInspector listInspector;

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length != 1) {
            throw new UDFArgumentException("last_index takes an array as an argument.");
        }
        if (objectInspectors[0].getCategory() != ObjectInspector.Category.LIST) {
            throw new UDFArgumentException("last_index takes an array as an argument.");
        }
        listInspector = (ListObjectInspector) objectInspectors[0];

        return listInspector.getListElementObjectInspector();
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        Object list = deferredObjects[0].get();
        int lastIdx = listInspector.getListLength(list) - 1;
        if (lastIdx >= 0) {
            Object unInsp = listInspector.getListElement(list, lastIdx);
            return unInsp;
        } else {
            return null;
        }
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "last_index( " + strings[0] + " )";
    }
}
