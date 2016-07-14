package shuyun.java.cds.udf.collect;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by endy on 2015/10/12.
 */
@Description(name = "map_filter_keys",
        value = "_FUNC_(map, key_array) - Returns the filtered entries of a map corresponding to a given set of keys "
)
public class MapFilterKeys extends GenericUDF {
    private MapObjectInspector mapInspector;
    private StandardMapObjectInspector retValInspector;
    private ListObjectInspector keyListInspector;

    private Map stdKeys(Map inspectMap) {
        Map objMap = new HashMap();
        for (Object inspKey : inspectMap.keySet()) {

            Object objKey = ((PrimitiveObjectInspector) mapInspector.getMapKeyObjectInspector()).getPrimitiveJavaObject(inspKey);
            objMap.put(objKey, inspKey);

        }
        return objMap;
    }


    private List inspectList(List inspectList) {
        List objList = new ArrayList();
        for (Object inspKey : inspectList) {

            Object objKey = ((PrimitiveObjectInspector) keyListInspector.getListElementObjectInspector()).getPrimitiveJavaObject(inspKey);

            objList.add(objKey);

        }
        return objList;
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        ObjectInspector first = objectInspectors[0];
        if (first.getCategory() == ObjectInspector.Category.MAP) {
            mapInspector = (MapObjectInspector) first;
        } else {
            throw new UDFArgumentException(" Expecting a map as first argument ");
        }

        ObjectInspector second = objectInspectors[1];
        if (second.getCategory() == ObjectInspector.Category.LIST) {
            keyListInspector = (ListObjectInspector) second;
        } else {
            throw new UDFArgumentException(" Expecting a list as second argument ");
        }

        //// List inspector ...
        if (!(keyListInspector.getListElementObjectInspector().getCategory() == ObjectInspector.Category.PRIMITIVE)) {
            throw new UDFArgumentException(" Expecting a primitive as key list elements.");
        }
        ObjectInspector mapKeyInspector = mapInspector.getMapKeyObjectInspector();
        if (!(mapKeyInspector.getCategory() == ObjectInspector.Category.PRIMITIVE)) {
            throw new UDFArgumentException(" Expecting a primitive as map key elements.");
        }

        if (((PrimitiveObjectInspector) keyListInspector.getListElementObjectInspector()).getPrimitiveCategory() != ((PrimitiveObjectInspector) mapKeyInspector).getPrimitiveCategory()) {
            throw new UDFArgumentException(" Expecting keys to be of same types.");
        }

        retValInspector = (StandardMapObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(first);
        return retValInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        Map hiveMap = mapInspector.getMap(deferredObjects[0].get());
        List keyValues = inspectList(keyListInspector.getList(deferredObjects[1].get()));
        /// Convert all the keys to standard keys
        Map stdKeys = stdKeys(hiveMap);

        Map retVal = (Map) retValInspector.create();
        for (Object keyObj : keyValues) {
            if (stdKeys.containsKey(keyObj)) {
                Object hiveKey = stdKeys.get(keyObj);
                Object hiveVal = hiveMap.get(hiveKey);
                Object keyStd = ObjectInspectorUtils.copyToStandardObject(hiveKey, mapInspector.getMapKeyObjectInspector());
                Object valStd = ObjectInspectorUtils.copyToStandardObject(hiveVal, mapInspector.getMapValueObjectInspector());

                retVal.put(keyStd, valStd);
            }
        }
        return retVal;
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "map_filter_keys(" + strings[0] + ", " + strings[1] + " )";
    }
}
