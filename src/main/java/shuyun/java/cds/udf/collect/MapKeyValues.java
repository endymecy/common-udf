package shuyun.java.cds.udf.collect;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by endy on 2015/10/12.
 */
@Description(name = "map_key_values",
        value = "_FUNC_(map) - Returns a Array of key-value pairs contained in a Map"
)
public class MapKeyValues extends GenericUDF{
    private MapObjectInspector moi;

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length != 1) {
            throw new UDFArgumentException("Usage : map_key_values( map) ");
        }
        if (!objectInspectors[0].getCategory().equals(ObjectInspector.Category.MAP)) {
            throw new UDFArgumentException("Usage : map_key_values( map) ");
        }

        moi = (MapObjectInspector) objectInspectors[0];

        ////
        List<String> structFieldNames = new ArrayList<String>();
        List<ObjectInspector> structFieldObjectInspectors = new ArrayList<ObjectInspector>();
        structFieldNames.add("key");
        structFieldObjectInspectors.add(moi.getMapKeyObjectInspector());
        structFieldNames.add("value");
        structFieldObjectInspectors.add(moi.getMapValueObjectInspector());

        ObjectInspector keyOI = ObjectInspectorFactory.getStandardStructObjectInspector(structFieldNames, structFieldObjectInspectors);
        ObjectInspector arrayOI = ObjectInspectorFactory.getStandardListObjectInspector(keyOI);

        return arrayOI;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        Map<?, ?> map = moi.getMap(deferredObjects[0].get());
        Object[] res = new Object[map.size()];
        int i = 0;
        for (Map.Entry e : map.entrySet()) {
            res[i++] = new Object[]{e.getKey(), e.getValue()};
        }
        return res;
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "map_key_values( " + strings[0] + " )";
    }
}
