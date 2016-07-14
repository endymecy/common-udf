package shuyun.java.cds.udf.collect;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;

/**
 * Created by endy on 2015/10/12.
 * 返回数组中给定index的值
 */
@Description(name = "array_index",
        value = "_FUNC_(array,index) - return value of given index"
)
public class ArrayIndex extends GenericUDF{
    private ListObjectInspector listInspector;
    private IntObjectInspector intInspector;

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length != 2) {
            throw new UDFArgumentException("array_index takes an array and an int as arguments");
        }
        if (objectInspectors[0].getCategory() != ObjectInspector.Category.LIST
                || objectInspectors[1].getCategory() != ObjectInspector.Category.PRIMITIVE
                || ((PrimitiveObjectInspector) objectInspectors[1]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.INT) {
            throw new UDFArgumentException("array_index takes an array and an int as arguments");
        }
        listInspector = (ListObjectInspector) objectInspectors[0];
        intInspector = (IntObjectInspector) objectInspectors[1];

        return listInspector.getListElementObjectInspector();
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        Object list = deferredObjects[0].get();
        int idx = intInspector.get(deferredObjects[1].get());

        if (idx < 0) {
            idx = listInspector.getListLength(list) + idx;
        }

        Object unInsp = listInspector.getListElement(list, idx);

        return unInsp;
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "array_index( " + strings[0] + " , " + strings[1] + " )";
    }
}
