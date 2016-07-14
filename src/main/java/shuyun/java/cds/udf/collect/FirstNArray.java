package shuyun.java.cds.udf.collect;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;

/**
 * Created by endy on 2015/10/12.
 */
@Description(name = "firstN_array",
        value = "_FUNC_(a,b) - Truncate an array, and only return the first N elements "
)
public class FirstNArray extends GenericUDF{
    private ListObjectInspector listInspector;
    private StandardListObjectInspector returnInspector;
    private IntObjectInspector intInspector;

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        ObjectInspector first = objectInspectors[0];
        if (first.getCategory() == ObjectInspector.Category.LIST) {
            listInspector = (ListObjectInspector) first;
        } else {
            throw new UDFArgumentException(" Expecting an array and an int as arguments ");
        }

        ObjectInspector second = objectInspectors[1];
        if (second.getCategory() == ObjectInspector.Category.PRIMITIVE) {
            PrimitiveObjectInspector secondPrim = (PrimitiveObjectInspector) second;
            if (secondPrim.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.INT) {
                intInspector = (IntObjectInspector) second;
            } else {
                throw new UDFArgumentException(" Expecting an array and an int as arguments ");
            }
        } else {
            throw new UDFArgumentException(" Expecting an array and an int as arguments ");
        }


        returnInspector = ObjectInspectorFactory.getStandardListObjectInspector(
                listInspector.getListElementObjectInspector());
        return returnInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        int numVals = intInspector.get(deferredObjects[1].get());
        Object uninspListObj = deferredObjects[0].get();
        int listSize = listInspector.getListLength(uninspListObj);

        Object truncatedListObj = returnInspector.create(numVals);
        for (int i = 0; i < numVals && i < listSize; ++i) {
            returnInspector.set(truncatedListObj, i, listInspector.getListElement(uninspListObj, i));
        }
        return truncatedListObj;
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "truncate_array(" + strings[0] + ", " + strings[1] + " )";
    }
}
