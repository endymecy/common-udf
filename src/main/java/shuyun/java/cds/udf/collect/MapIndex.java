package shuyun.java.cds.udf.collect;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

import java.util.Map;

/**
 * Created by endy on 2015/10/12.
 */
@Description(name = "map_index",
        value = "_FUNC_(map, key_map) - Returns index of a map corresponding to a given set of keys "
)
public class MapIndex extends GenericUDF {
    private PrimitiveObjectInspector keyInspector;
    private MapObjectInspector mapInspector;
    private PrimitiveObjectInspector mapKeyInspector;
    private CreateWithPrimitive createKey;

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length != 2) {
            throw new UDFArgumentException("Usage : map_index( map, key)");
        }
        if (objectInspectors[0].getCategory() != ObjectInspector.Category.MAP
                || objectInspectors[1].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentException("Usage : map_index( map, key) - First argument must be a map, second must be a matching key");
        }
        mapInspector = (MapObjectInspector) objectInspectors[0];
        mapKeyInspector = (PrimitiveObjectInspector) mapInspector.getMapKeyObjectInspector();
        keyInspector = (PrimitiveObjectInspector) objectInspectors[1];
        if (mapKeyInspector.getPrimitiveCategory() != keyInspector.getPrimitiveCategory()) {
            throw new UDFArgumentException("Usage : map_index( map, key) - First argument must be a map, second must be a matching key");
        }
        createKey = CreateWithPrimitive.getCreate(mapKeyInspector);
        return mapInspector.getMapValueObjectInspector();
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        Map<?, ?> map = mapInspector.getMap(deferredObjects[0].get());
        Object key = keyInspector.getPrimitiveJavaObject(deferredObjects[1].get());
        if (key == null) {
            return map.get(null);
        }
        if (createKey != null) {
            return map.get(createKey.create(key));
        }
        for (Map.Entry<?, ?> e : map.entrySet()) {
            if (key.equals(mapKeyInspector.getPrimitiveJavaObject(e.getKey()))) {
                return e.getValue();
            }
        }
        return null;
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "map_index( " + strings[0] + " , " + strings[1] + ")";
    }
}
