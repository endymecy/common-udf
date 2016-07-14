package shuyun.java.cds.udf.collect;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.Map;
import java.util.TreeMap;

/**
 * Created by endy on 2015/10/12.
 * 将map中的对象转换为另外的类型
 */
@Description(name = "cast_map",
        value = "_FUNC_(map,type) - Returns the union of a set of arrays "
)
public class CastMap extends GenericUDF {
    private MapObjectInspector mapInspector;

    public Map<String, String> evaluate(Map<Object, Object> strMap) {
        Map<String, String> newMap = new TreeMap<String, String>();
        for (Object keyObj : strMap.keySet()) {
            newMap.put(keyObj.toString(), strMap.get(keyObj).toString());
        }
        return newMap;
    }
    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        this.mapInspector = (MapObjectInspector) objectInspectors[0];

        ObjectInspector returnType = ObjectInspectorFactory.getStandardMapObjectInspector(
                PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return returnType;
    }

    @Override
    public Map<String, String> evaluate(DeferredObject[] arg0) throws HiveException {
        Map argMap = mapInspector.getMap(arg0[0].get());
        if (argMap != null)
            return evaluate(argMap);
        else
            return null;
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "cast_map()";
    }
}
