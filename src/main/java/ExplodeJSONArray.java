import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;

import java.util.ArrayList;

public class ExplodeJSONArray extends GenericUDTF {
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        if (argOIs.getAllStructFieldRefs().size()!=1) {
            throw new UDFArgumentLengthException("explode_json_array函数的参数个数只能为1");
        }
        String typeName = argOIs.getAllStructFieldRefs().get(0).getFieldObjectInspector().getTypeName();
        if (!"string".equals(typeName)){
            throw new UDFArgumentTypeException(0,"explode_json_array函数的第1个参数的类型只能为String...");
        }
        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fidldOIS = new ArrayList<>();
        fieldNames.add("item");
        fidldOIS.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fidldOIS);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        String jsonArrayStr = args[0].toString();
        JSONArray jsonArray = new JSONArray(jsonArrayStr);
        for (int i =0;i<jsonArray.length();i++){
            String jsonStr = jsonArray.getString(i);
            String[] result = new String[1];
            result[0]=jsonStr;
            forward(result);
        }
    }

    @Override
    public void close() throws HiveException {

    }
}
