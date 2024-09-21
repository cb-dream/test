package com.cb.kafka.produce;


import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class MyUDF extends GenericUDF {
    @Override
    public ObjectInspector initialize(ObjectInspector[] a) throws UDFArgumentException {
        if ( a == null || a.length != 1 )
            throw new UDFArgumentException("MyUDF requires exactly one argument");
        if ( a[0].getCategory() != ObjectInspector.Category.PRIMITIVE )
            throw new UDFArgumentException("MyUDF requires a primitive argument");
        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] a) throws HiveException {
        Object o = a[0].get();
        if ( o == null )
            return 0;
        return o.toString().length();
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "";
    }
}
