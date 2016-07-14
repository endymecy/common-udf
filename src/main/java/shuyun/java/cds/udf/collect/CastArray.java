package shuyun.java.cds.udf.collect;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by endy on 2015/10/12.
 * 将一个数组的对象转换成另外类型的对象
 */

@Description(name = "cast_array",
        value = "_FUNC_(array,type) - Returns the union of a set of arrays "
)
public class CastArray extends GenericUDF{
    private ListObjectInspector listInspector;
    private PrimitiveObjectInspector fromInspector;
    private PrimitiveObjectInspector toInspector;
    private String returnElemType;

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors[0].getCategory() != ObjectInspector.Category.LIST) {
            throw new UDFArgumentException("cast_array() takes a list, and an optional type to cast to.");
        }
        this.listInspector = (ListObjectInspector) objectInspectors[0];
        if (listInspector.getListElementObjectInspector().getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentException("cast_array() only handles arrays of primitives.");
        }
        this.fromInspector = (PrimitiveObjectInspector) listInspector.getListElementObjectInspector();

        if (objectInspectors.length > 1) {
            if (!(objectInspectors[1] instanceof ConstantObjectInspector)
                    || !(objectInspectors[1] instanceof StringObjectInspector)) {
                throw new UDFArgumentException("cast_array() takes a list, and an optional type to cast to.");
            }
            ConstantObjectInspector constInsp = (ConstantObjectInspector) objectInspectors[1];
            this.returnElemType = constInsp.getWritableConstantValue().toString();
            this.toInspector = GetObjectInspectorForTypeName(returnElemType);
            ObjectInspector returnType = ObjectInspectorFactory.getStandardListObjectInspector(toInspector);
            return returnType;
        }

        /// Otherwise, assume we're casting to strings ...
        this.returnElemType = "string";
        this.toInspector = GetObjectInspectorForTypeName(returnElemType);
        ObjectInspector returnType = ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return returnType;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        List argList = listInspector.getList(deferredObjects[0].get());
        if (argList != null)
            return evaluate(argList);
        else
            return null;
    }

    @Override
    public String getDisplayString(String[] strings) {
        StringBuilder sb = new StringBuilder("cast_array(");
        sb.append(strings[0]);
        if (strings.length > 1) {
            sb.append(" , ");
            sb.append(strings[1]);
        }
        return sb.toString();
    }


    public List<Object> evaluate(List<Object> uninspArray) {
        List<Object> newList = new ArrayList<Object>();
        for (Object uninsp : uninspArray) {
            Object stdObject = ObjectInspectorUtils.copyToStandardJavaObject(uninsp, fromInspector);
            Object castedObject = coerceObject(stdObject);
            newList.add(castedObject);
        }
        return newList;
    }

    private Object coerceObject(Object stdObj) {
        if (stdObj == null) {
            return null;
        }
        switch (fromInspector.getPrimitiveCategory()) {
            case STRING:
                String fromStr = (String) stdObj;
                switch (toInspector.getPrimitiveCategory()) {
                    case STRING:
                        return fromStr;
                    case BOOLEAN:
                        if (fromStr.equals("true")) {
                            return Boolean.TRUE;
                        } else {
                            return Boolean.FALSE;
                        }
                    case BYTE:
                        /// XXX TODO
                    case SHORT:
                        return Short.parseShort(fromStr);
                    case INT:
                        return Integer.parseInt(fromStr);
                    case LONG:
                        return Long.parseLong(fromStr);
                    case FLOAT:
                        return Float.parseFloat(fromStr);
                    case DOUBLE:
                        return Double.parseDouble(fromStr);
                    case TIMESTAMP:
                        //// XXX TODO
                    case VOID:
                        return null;

                }
                return null;
            case SHORT:
            case INT:
            case FLOAT:
            case LONG:
            case DOUBLE:
                Number fromNum = (Number) stdObj;
                switch (toInspector.getPrimitiveCategory()) {
                    case SHORT:
                        return fromNum.shortValue();
                    case INT:
                        return fromNum.intValue();
                    case LONG:
                        return fromNum.longValue();
                    case FLOAT:
                        return fromNum.floatValue();
                    case DOUBLE:
                        return fromNum.doubleValue();
                    case STRING:
                        return fromNum.toString();
                    case TIMESTAMP:
                        //// XXX TODO
                    case VOID:
                        return null;
                }
                return null;
        }
        return null;
    }

    private static PrimitiveObjectInspector GetObjectInspectorForTypeName(String typeString) {
        TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(typeString);
        return (PrimitiveObjectInspector) TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(typeInfo);
    }

}
