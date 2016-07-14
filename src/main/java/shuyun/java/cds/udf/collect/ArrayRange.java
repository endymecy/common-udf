package shuyun.java.cds.udf.collect;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;

/**
 * Created by endy on 2015/10/12.
 * 获取给定范围的数组的子集
 */
@Description(name = "array_range",
        value = "_FUNC_(array, start,end) - Returns sub array of given array"
)
public class ArrayRange extends GenericUDF{
    private ListObjectInspector listInspector;
    private StandardListObjectInspector returnInspector;
    private IntObjectInspector firstIntInspector, secondIntInspector;

    public int[] getIndexes(DeferredObject[] arg0) throws HiveException {
        int start, end, length;
        if (secondIntInspector == null) {
            start = 0;
            end = firstIntInspector.get(arg0[1].get());
            length = end;
        } else {
            start = firstIntInspector.get(arg0[1].get());
            end = secondIntInspector.get(arg0[2].get());
            length = end - start + 1;
        }

        return new int[]{start, end, length};
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        ObjectInspector first = objectInspectors[0];
        if (first.getCategory() == ObjectInspector.Category.LIST) {
            listInspector = (ListObjectInspector) first;
        } else {
            throw new UDFArgumentException(" Expecting an array, one int and one optional int as arguments ");
        }

        ObjectInspector second = objectInspectors[1];
        if (second.getCategory() == ObjectInspector.Category.PRIMITIVE) {
            PrimitiveObjectInspector secondPrim = (PrimitiveObjectInspector) second;
            if (secondPrim.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.INT) {
                firstIntInspector = (IntObjectInspector) second;
            } else {
                throw new UDFArgumentException(" Expecting an array, one int and one optional int as arguments ");
            }
        } else {
            throw new UDFArgumentException(" Expecting an array, one int and one optional int as arguments ");
        }

        if (objectInspectors.length > 2) {

            ObjectInspector third = objectInspectors[2];
            if (third != null) {
                if (third.getCategory() == ObjectInspector.Category.PRIMITIVE) {
                    PrimitiveObjectInspector thirdPrim = (PrimitiveObjectInspector) third;
                    if (thirdPrim.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.INT) {
                        secondIntInspector = (IntObjectInspector) third;
                    } else {
                        throw new UDFArgumentException(" Expecting an array, one int and one optional int as arguments ");
                    }
                } else {
                    throw new UDFArgumentException(" Expecting an array, one int and one optional int as arguments ");
                }
            }
        }
        returnInspector = ObjectInspectorFactory.getStandardListObjectInspector(
                listInspector.getListElementObjectInspector());
        return returnInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        int indexes[] = getIndexes(deferredObjects);
        int start = indexes[0];
        int end = indexes[1];
        int length = indexes[2];
        Object uninspListObj = deferredObjects[0].get();
        int listSize = listInspector.getListLength(uninspListObj);
        Object truncatedListObj = returnInspector.create(length);
        for (int i = 0; i < end && i < listSize; ++i) {
            returnInspector.set(truncatedListObj, i, listInspector.getListElement(uninspListObj, i + start));
        }
        return truncatedListObj;
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "array_range(" + strings[0] + ", " + strings[1] + " , " + strings[1] + " )";
    }
}
