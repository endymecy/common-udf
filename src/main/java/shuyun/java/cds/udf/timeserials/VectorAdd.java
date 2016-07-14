package shuyun.java.cds.udf.timeserials;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by endy on 2015/10/13.
 * 将两个vector合并在一起
 */
@Description(
        name = "vector_add",
        value = " Add two vectors together"
)
public class VectorAdd extends GenericUDF {
    private ListObjectInspector list1Inspector;
    private ListObjectInspector list2Inspector;
    private MapObjectInspector map1Inspector;
    private MapObjectInspector map2Inspector;
    private PrimitiveObjectInspector key1Inspector;
    private PrimitiveObjectInspector key2Inspector;
    private PrimitiveObjectInspector value1Inspector;
    private PrimitiveObjectInspector value2Inspector;

    private StandardListObjectInspector retListInspector;
    private StandardMapObjectInspector retMapInspector;
    public Object evaluateList(Object list1Obj, Object list2Obj) {
        int len1 = list1Inspector.getListLength(list1Obj);
        int len2 = list2Inspector.getListLength(list2Obj);
        if (len1 != len2) {
            return null;
        }
        Object retList = retListInspector.create(0);
        for (int i = 0; i < len1; ++i) {
            Object list1Val = this.list1Inspector.getListElement(list1Obj, i);
            double list1Dbl = NumericUtil.getNumericValue(value1Inspector, list1Val);
            Object list2Val = this.list2Inspector.getListElement(list2Obj, i);
            double list2Dbl = NumericUtil.getNumericValue(value2Inspector, list2Val);

            double newVal = list1Dbl + list2Dbl;
            retListInspector.set(retList, i, NumericUtil.castToPrimitiveNumeric(newVal,
                    ((PrimitiveObjectInspector) retListInspector.getListElementObjectInspector()).getPrimitiveCategory()));
        }
        return retList;
    }

    public Object evaluateMap(Object uninspMapObj1, Object uninspMapObj2) {
        /// A little tricky, because keys won't match if the ObjectInspectors aren't the
        /// same .. If the ObjectInspectors are the same class, assume they can be compared
        Object retMap = retMapInspector.create();
        Map map1 = map1Inspector.getMap(uninspMapObj1);
        Map map2 = map2Inspector.getMap(uninspMapObj2);
        Map stdKeyMap = new HashMap();
        for (Object mapKey2 : map2.keySet()) {
            Object stdKey2 = ObjectInspectorUtils.copyToStandardJavaObject(mapKey2,
                    map2Inspector.getMapKeyObjectInspector());
            stdKeyMap.put(stdKey2, mapKey2);
        }

        for (Object mapKey1 : map1.keySet()) {
            Object mapVal1Obj = map1.get(mapKey1);
            double mapVal1Dbl = NumericUtil.getNumericValue(value1Inspector, mapVal1Obj);

            Object stdKey1 = ObjectInspectorUtils.copyToStandardJavaObject(mapKey1,
                    map1Inspector.getMapKeyObjectInspector());

            Object lookupKey = stdKeyMap.get(stdKey1);

            if (lookupKey != null) {
                Object mapVal2Obj = map2.get(lookupKey);
                double mapVal2Dbl = NumericUtil.getNumericValue(value2Inspector, mapVal2Obj);
                double newVal = mapVal1Dbl + mapVal2Dbl;
                stdKeyMap.remove(stdKey1);

                Object stdVal = NumericUtil.castToPrimitiveNumeric(newVal,
                        ((PrimitiveObjectInspector) retMapInspector.getMapValueObjectInspector()).getPrimitiveCategory());
                retMapInspector.put(retMap, stdKey1, stdVal);
            } else {
                /// Add the dimension, even if it wasn't in the second vector
                Object stdVal = NumericUtil.castToPrimitiveNumeric(mapVal1Dbl,
                        ((PrimitiveObjectInspector) retMapInspector.getMapValueObjectInspector()).getPrimitiveCategory());
                retMapInspector.put(retMap, stdKey1, stdVal);
            }

        }
        /// Add all the values which were in map2, but not in map1
        for (Object leftOverKey : stdKeyMap.keySet()) {
            Object leftOverVal = map2.get(stdKeyMap.get(leftOverKey));
            double leftOverDbl = NumericUtil.getNumericValue(value2Inspector, leftOverVal);
            Object stdVal = NumericUtil.castToPrimitiveNumeric(leftOverDbl,
                    ((PrimitiveObjectInspector) retMapInspector.getMapValueObjectInspector()).getPrimitiveCategory());
            retMapInspector.put(retMap, leftOverKey, stdVal);
        }
        return retMap;
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length != 2)
            usage("Must have two arguments.");

        if (objectInspectors[0].getCategory() == ObjectInspector.Category.MAP) {
            if (objectInspectors[1].getCategory() != ObjectInspector.Category.MAP)
                usage("Vectors need to be both maps");
            this.map1Inspector = (MapObjectInspector) objectInspectors[0];
            this.map2Inspector = (MapObjectInspector) objectInspectors[1];

            if (map1Inspector.getMapKeyObjectInspector().getCategory() != ObjectInspector.Category.PRIMITIVE)
                usage("First Vector map key must be a primitive");
            this.key1Inspector = (PrimitiveObjectInspector) map1Inspector.getMapKeyObjectInspector();

            if (map2Inspector.getMapKeyObjectInspector().getCategory() != ObjectInspector.Category.PRIMITIVE)
                usage("Second Vector map key must be a primitive");
            this.key2Inspector = (PrimitiveObjectInspector) map2Inspector.getMapKeyObjectInspector();

            if (key2Inspector.getPrimitiveCategory() != key1Inspector.getPrimitiveCategory())
                usage(" Map key types must match");

            if (map1Inspector.getMapValueObjectInspector().getCategory() != ObjectInspector.Category.PRIMITIVE)
                usage("First Vector map value must be a primitive");
            this.value1Inspector = (PrimitiveObjectInspector) map1Inspector.getMapValueObjectInspector();

            if (map2Inspector.getMapValueObjectInspector().getCategory() != ObjectInspector.Category.PRIMITIVE)
                usage("Second Vector map value must be a primitive");
            this.value2Inspector = (PrimitiveObjectInspector) map2Inspector.getMapValueObjectInspector();


        } else if (objectInspectors[0].getCategory() == ObjectInspector.Category.LIST) {
            if (objectInspectors[1].getCategory() != ObjectInspector.Category.LIST)
                usage("Vectors need to be both arrays");
            this.list1Inspector = (ListObjectInspector) objectInspectors[0];
            this.list2Inspector = (ListObjectInspector) objectInspectors[1];

            if (list1Inspector.getListElementObjectInspector().getCategory() != ObjectInspector.Category.PRIMITIVE)
                usage("First Vector array value must be a primitive");
            this.value1Inspector = (PrimitiveObjectInspector) list1Inspector.getListElementObjectInspector();

            if (list2Inspector.getListElementObjectInspector().getCategory() != ObjectInspector.Category.PRIMITIVE)
                usage("Second Vector array value must be a primitive");
            this.value2Inspector = (PrimitiveObjectInspector) list2Inspector.getListElementObjectInspector();

        } else {
            usage("Arguments must be arrays or maps");
        }


        if (list1Inspector != null) {
            retListInspector = ObjectInspectorFactory.getStandardListObjectInspector(
                    ObjectInspectorUtils.getStandardObjectInspector(value1Inspector,
                            ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA));
            return retListInspector;
        } else {
            retMapInspector = ObjectInspectorFactory.getStandardMapObjectInspector(
                    ObjectInspectorUtils.getStandardObjectInspector(map1Inspector.getMapKeyObjectInspector(),
                            ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA),
                    ObjectInspectorUtils.getStandardObjectInspector(value1Inspector,
                            ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA));
            return retMapInspector;
        }
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        if (list1Inspector != null) {
            return evaluateList(deferredObjects[0].get(), deferredObjects[1].get());
        } else {
            return evaluateMap(deferredObjects[0].get(), deferredObjects[1].get());
        }
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "vector_cross_product";
    }

    private void usage(String message) throws UDFArgumentException {
        throw new UDFArgumentException("vector_scalar_mult: Multiply a vector times another vector : " + message);
    }
}
