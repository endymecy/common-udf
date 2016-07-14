package shuyun.java.cds.udf.collect;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.*;

import java.util.List;

/**
 * Created by endy on 2015/10/12.
 * 将一个对象添加到array的最后
 */

@Description(
        name = "append_array",
        value = "_FUNC_(arg1,arg2)- Append an object to the end of an Array"
)
public class AppendArray extends GenericUDF {
    private ListObjectInspector listInspector;
    private PrimitiveObjectInspector listElemInspector;
    private boolean returnWritables;
    private PrimitiveObjectInspector primInspector;

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        try {
            listInspector = (ListObjectInspector) objectInspectors[0];
            listElemInspector = (PrimitiveObjectInspector) listInspector.getListElementObjectInspector();
            primInspector = (PrimitiveObjectInspector) objectInspectors[1];
            if (listElemInspector.getPrimitiveCategory() != primInspector.getPrimitiveCategory()) {
                throw new UDFArgumentException(
                        "append_array expects the list type to match the type of the value being appended");
            }
            returnWritables = listElemInspector.preferWritable();
            return ObjectInspectorFactory.getStandardListObjectInspector(
                    ObjectInspectorUtils.getStandardObjectInspector(listElemInspector));
        } catch (ClassCastException e) {
            throw new UDFArgumentException("append_array expects a list as the first argument and a primitive " +
                    "as the second argument and the list type to match the type of the value being appended");
        }
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        List objList = listInspector.getList(deferredObjects[0].get());
        Object objToAppend = deferredObjects[1].get();
        Object[] res = new Object[objList.size() + 1];
        for (int i = 0; i < objList.size(); i++) {
            Object o = objList.get(i);
            res[i] = returnWritables ?
                    listElemInspector.getPrimitiveWritableObject(o) :
                    listElemInspector.getPrimitiveJavaObject(o);
        }
        res[res.length - 1] = returnWritables ?
                primInspector.getPrimitiveWritableObject(objToAppend) :
                primInspector.getPrimitiveJavaObject(objToAppend);
        return res;
    }

    @Override
    public String getDisplayString(String[] args) {
        return "append_array(" + args[0] + ", " + args[1] + ")";
    }
}
