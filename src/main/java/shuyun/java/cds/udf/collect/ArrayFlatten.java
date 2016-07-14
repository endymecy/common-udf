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
@Description(name = "array_flatten",
        value = "_FUNC_(array) - Returns the array with the elements flattened."
)
public class ArrayFlatten extends GenericUDF{
    private ListObjectInspector listInspector;
    private StandardListObjectInspector returnInspector;
    private IntObjectInspector depthIntInspector;

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length != 1 && objectInspectors.length != 2) { // for now, array_flatten(array). Later array_flatten(array, depth)
            throw new UDFArgumentException("array_flatten : expected format array_flatten(array) or array_flatten(array, depth)");
        }

        ObjectInspector list = objectInspectors[0];

        if (list.getCategory() != ObjectInspector.Category.LIST) {
            throw new UDFArgumentException("array_flatten : expecting array as input, got " + list.getTypeName());
        }

        listInspector = (ListObjectInspector) list;

        if (objectInspectors.length == 2) {
            ObjectInspector depth = objectInspectors[1];
            PrimitiveObjectInspector depthInsp = (PrimitiveObjectInspector) depth;
            if (depthInsp.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.INT) {
                depthIntInspector = (IntObjectInspector) depth;//遍历深度
            } else {
                throw new UDFArgumentException("array_flatten : expecting optional second parameter as INT");
            }
        }

        if (listInspector.getListElementObjectInspector().getCategory() == ObjectInspector.Category.PRIMITIVE) {
            returnInspector = ObjectInspectorFactory.getStandardListObjectInspector(
                    listInspector.getListElementObjectInspector());
        } else {
            ListObjectInspector subArrayInspector = (ListObjectInspector) listInspector.getListElementObjectInspector();

            returnInspector = ObjectInspectorFactory.getStandardListObjectInspector(
                    subArrayInspector.getListElementObjectInspector());
        }
        return returnInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        Object inputObject = deferredObjects[0].get();

        if (deferredObjects.length != 1 || inputObject == null) return null;

        //first, check if we are already flattened
        ObjectInspector elementInspector = listInspector.getListElementObjectInspector();
        if (elementInspector.getCategory() == ObjectInspector.Category.PRIMITIVE) return inputObject;

        //second, get the length of the resulting flat
        int inputLength = listInspector.getListLength(inputObject);
        int resultLength = 0;
        ListObjectInspector subArrayInspector = (ListObjectInspector) elementInspector;
        for (int i = 0; i < inputLength; i++) {
            resultLength += subArrayInspector.getListLength(listInspector.getListElement(inputObject, i));
        }


        //now, flatten the list by one level.
        Object flattenedListObj = returnInspector.create(resultLength);

        int resultIndex = 0;

        for (int i = 0; i < inputLength; i++) {
            Object element = listInspector.getListElement(inputObject, i);
            int subArrayLength = subArrayInspector.getListLength(element);
            for (int j = 0; j < subArrayLength; j++) {
                Object subArrayElement = subArrayInspector.getListElement(element, j);
                returnInspector.set(flattenedListObj, resultIndex, subArrayElement);
                ++resultIndex;
            }
        }

        return flattenedListObj;
    }

    @Override
    public String getDisplayString(String[] strings) {
        String display = "array_flatten(" + strings[0];
        if (strings.length == 1) {
            display.concat(")");
        } else {
            display.concat(", " + strings[1].toString() + ")");
        }
        return display;
    }
}
