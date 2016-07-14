package shuyun.java.cds.udf.collect;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;

/**
 * Created by endy on 2015/10/12.
 * 在数组中找到给定对象的index
 */

@Description(name = "array_index_of",
        value = "_FUNC_(array,value) - return index of given value"
)
public class ArrayIndexOf extends GenericUDF{
    private static final int ARRAY_IDX = 0;
    private static final int VALUE_IDX = 1;
    private static final int ARG_COUNT = 2; // Number of arguments to this UDF
    private static final String FUNC_NAME = "ARRAY_INDEX_OF"; // External Name

    private ObjectInspector valueOI;
    private ListObjectInspector arrayOI;
    private ObjectInspector arrayElementOI;
    private IntWritable result;

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        //检查是否传人了两个参数
        if (objectInspectors.length != ARG_COUNT) {
            throw new UDFArgumentException("The function " + FUNC_NAME + " accepts " + ARG_COUNT + " arguments.");
        }

        //检查第一个参数是否是LIST
        if (!objectInspectors[ARRAY_IDX].getCategory().equals(ObjectInspector.Category.LIST)) {
            throw new UDFArgumentTypeException(ARRAY_IDX, "\"" + "LIST"
                    + "\" " + "expected at function " + FUNC_NAME + ", but " + "\"" + objectInspectors[ARRAY_IDX].getTypeName()
                    + "\" " + "is found");
        }

        arrayOI = (ListObjectInspector) objectInspectors[ARRAY_IDX];
        arrayElementOI = arrayOI.getListElementObjectInspector();

        valueOI = objectInspectors[VALUE_IDX];

        //检查list的元素和值是相同的类型
        if (!ObjectInspectorUtils.compareTypes(arrayElementOI, valueOI)) {
            throw new UDFArgumentTypeException(VALUE_IDX, "\"" + arrayElementOI.getTypeName() + "\""
                    + " expected at function " + FUNC_NAME + ", but " + "\"" + valueOI.getTypeName() + "\"" + " is found");
        }

        //检查比较的类型是否支持
        if (!ObjectInspectorUtils.compareSupported(valueOI)) {
            throw new UDFArgumentException("The function " + FUNC_NAME + " does not support comparison for " + "\""
                    + valueOI.getTypeName() + "\"" + " types");
        }

        result = new IntWritable(-1);

        return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        result.set(-1);

        Object array = deferredObjects[ARRAY_IDX].get();
        Object value = deferredObjects[VALUE_IDX].get();

        int arrayLength = arrayOI.getListLength(array);

        // Check if array is null or empty or value is null
        if (value == null || arrayLength <= 0) {
            return result;
        }

        // Compare the value to each element of array until a match is found
        for (int i = 0; i < arrayLength; ++i) {
            Object listElement = arrayOI.getListElement(array, i);
            if (listElement != null) {
                if (ObjectInspectorUtils.compare(value, valueOI, listElement, arrayElementOI) == 0) {
                    result.set(i);
                    break;
                }
            }
        }

        return result;
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "array_index_of(" + strings[ARRAY_IDX] + ", " + strings[VALUE_IDX] + ")";
    }
}
