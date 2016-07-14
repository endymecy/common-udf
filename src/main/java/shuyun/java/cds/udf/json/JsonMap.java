package shuyun.java.cds.udf.json;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

/**
 * Created by endy on 2015/10/13.
 */
@Description(name = "json_map",
        value = "_FUNC_(json) - Returns a map of key-value pairs from a JSON string"
)
public class JsonMap extends GenericUDF {
    private StringObjectInspector stringInspector;
    private InspectorHandle inspHandle;
    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length != 1 && objectInspectors.length != 2) {
            throw new UDFArgumentException("Usage : json_map( jsonstring, optional typestring ) ");
        }
        if (!objectInspectors[0].getCategory().equals(ObjectInspector.Category.PRIMITIVE)
                || ((PrimitiveObjectInspector) objectInspectors[0]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentException("Usage : json_map( jsonstring, optional typestring) ");
        }

        stringInspector = (StringObjectInspector) objectInspectors[0];

        if (objectInspectors.length > 1) {
            if (!objectInspectors[1].getCategory().equals(ObjectInspector.Category.PRIMITIVE)
                    && ((PrimitiveObjectInspector) objectInspectors[1]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
                throw new UDFArgumentException("Usage : json_map( jsonstring, optional typestring) ");
            }
            if (!(objectInspectors[1] instanceof ConstantObjectInspector)) {
                throw new UDFArgumentException("json_map( jsonstring, typestring ) : typestring must be a constant");
            }
            ConstantObjectInspector constInsp = (ConstantObjectInspector) objectInspectors[1];
            String typeStr = ((Text) constInsp.getWritableConstantValue()).toString();

            String[] types = typeStr.split(",");
            if (types.length != 2) {
                throw new UDFArgumentException(" typestring must be of the form <keytype>,<valuetype>");
            }
            TypeInfo keyType = TypeInfoUtils.getTypeInfoFromTypeString(types[0]);
            TypeInfo valType = TypeInfoUtils.getTypeInfoFromTypeString(types[1]);

            ObjectInspector keyInsp = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(keyType);
            ObjectInspector valInsp = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(valType);

            MapObjectInspector mapInsp = ObjectInspectorFactory.getStandardMapObjectInspector(keyInsp, valInsp);

            inspHandle = InspectorHandle.InspectorHandleFactory.GenerateInspectorHandle(mapInsp);

            return inspHandle.getReturnType();

        } else {
            ObjectInspector keyInsp = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
            ObjectInspector valueInsp = PrimitiveObjectInspectorFactory.javaDoubleObjectInspector; /// XXX Make value type configurable somehow

            MapObjectInspector mapInsp = ObjectInspectorFactory.getStandardMapObjectInspector(keyInsp, valueInsp);

            inspHandle = InspectorHandle.InspectorHandleFactory.GenerateInspectorHandle(mapInsp);

            return inspHandle.getReturnType();
        }
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        try {
            String jsonString = this.stringInspector.getPrimitiveJavaObject(deferredObjects[0].get());
            //// Logic is the same as "from_json"
            ObjectMapper om = new ObjectMapper();
            JsonNode jsonNode = om.readTree(jsonString);
            return inspHandle.parseJson(jsonNode);

        } catch (JsonProcessingException e) {
            throw new HiveException(e);
        } catch (IOException e) {
            throw new HiveException(e);
        }
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "json_map( " + strings[0] + " )";
    }
}
