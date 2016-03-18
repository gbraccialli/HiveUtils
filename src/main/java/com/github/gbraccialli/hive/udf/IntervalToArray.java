package com.github.gbraccialli.hive.udf;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Reporter;

import java.util.Collections;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;


@Description(name = "IntervalToArray",
value = "_FUNC_( ...)  " +
		"  ")
public class IntervalToArray extends GenericUDF {
	
	static final Log LOG = LogFactory.getLog(IntervalToArray.class.getName());

	private PrimitiveObjectInspector fromOI;
	private PrimitiveObjectInspector toOI;
	private int interval;

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments)
			throws UDFArgumentException {

		fromOI = (PrimitiveObjectInspector) arguments[0];
		toOI = (PrimitiveObjectInspector) arguments[1];
		interval = Integer.valueOf(((ConstantObjectInspector)arguments[2]).getWritableConstantValue().toString());
		
		return ObjectInspectorFactory.getStandardListObjectInspector(
				PrimitiveObjectInspectorFactory.writableLongObjectInspector
				);
	    
	}

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {

		
		Object fromObj = arguments[0].get();
		Object toObj = arguments[1].get();
		
		if (fromObj == null || toObj == null){
			return null;
		}
				
		long from = PrimitiveObjectInspectorUtils.getLong(fromObj, fromOI);
		long to = PrimitiveObjectInspectorUtils.getLong(toObj, toOI);

		if (from > to){
			throw new HiveException("from value > to value");
		}

		int slots = ((int)(to - from)/interval)+1;
		ArrayList returnList = new ArrayList(slots);
		
		for (int i=0; i < slots; i++){
			returnList.add(new LongWritable(from));
			from+=interval;
		}
		
		return returnList;
	}

	
	@Override
	public String getDisplayString(String[] children) {
		return "IntervalToArray";
	}
	
}