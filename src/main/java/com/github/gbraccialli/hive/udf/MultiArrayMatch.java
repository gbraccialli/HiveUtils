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


@Description(name = "MultiArrayMatch",
value = "_FUNC_( ...)  " +
		"  ")
public class MultiArrayMatch extends GenericUDF {
	
	static final Log LOG = LogFactory.getLog(MultiArrayMatch.class.getName());

	private ListObjectInspector arrayValuesOI;
	private PrimitiveObjectInspector elementValueOI;
	
	private ListObjectInspector arrayConditionsOI;
	private StructObjectInspector structConditionsElementOI;
	private PrimitiveObjectInspector conditionNumberOI;
	private ListObjectInspector arrayConditionsValuesOI;
	private PrimitiveObjectInspector conditionValueOI;
	private PrimitiveObjectInspector valueKeyOI;
	private PrimitiveObjectInspector conditionsKeyOI;
	
	private transient String previousValueKey = "";
	private transient String previousConditionsKey = "";
	
	private transient int[] values;
	private transient HashMap<String,CachedConditions> cachedConditions = new HashMap<String, CachedConditions>();
	private transient CachedConditions currentConditions;
	
	private transient boolean cacheEnabled = false;
	

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments)
			throws UDFArgumentException {

		arrayValuesOI = (ListObjectInspector) arguments[0];
		elementValueOI = (PrimitiveObjectInspector)arrayValuesOI.getListElementObjectInspector();
		
		arrayConditionsOI = (ListObjectInspector) arguments[1];
		structConditionsElementOI = (StructObjectInspector)arrayConditionsOI.getListElementObjectInspector();

		conditionNumberOI = (PrimitiveObjectInspector)structConditionsElementOI.getStructFieldRef("position").getFieldObjectInspector();
		arrayConditionsValuesOI = (ListObjectInspector)structConditionsElementOI.getStructFieldRef("values").getFieldObjectInspector();
		
		conditionValueOI = (PrimitiveObjectInspector)arrayConditionsValuesOI.getListElementObjectInspector();
				
		if (arguments.length == 4){
			valueKeyOI = (PrimitiveObjectInspector) arguments[2];
			conditionsKeyOI = (PrimitiveObjectInspector) arguments[3];
			cacheEnabled = true;
		}
		
	    return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;

	}

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {

		
		Object arrayValues = arguments[0].get();
		Object arrayConditions = arguments[1].get();

		if (cacheEnabled){
			
			Object valueKeyObj = arguments[2].get();
			Object conditionsKeyObj = arguments[3].get();
			
			String valueKey;
			if (valueKeyObj == null)
				valueKey = "";
			else
				valueKey = PrimitiveObjectInspectorUtils.getString(valueKeyObj, valueKeyOI);
			
			if (!valueKey.equals(previousValueKey)){
				loadValues(arrayValues);
			}
			
			String conditionsKey;
			if (conditionsKeyObj == null) 
				conditionsKey = "";
			else
				conditionsKey = PrimitiveObjectInspectorUtils.getString(conditionsKeyObj, conditionsKeyOI);
			
			if (!conditionsKey.equals(previousConditionsKey)){
				loadConditions(arrayConditions, conditionsKey);
			}
			
			previousConditionsKey = conditionsKey;
			previousValueKey = valueKey;

		}else{
			loadValues(arrayValues);
			loadConditions(arrayConditions);
		}
		
		return new BooleanWritable(currentConditions.contains(values));
	}

	
	private void loadValues(Object arrayValues){
		int arrayValuesLength = arrayValuesOI.getListLength(arrayValues);
		values = new int[arrayValuesLength];
		
		for (int i=0; i < arrayValuesLength; i++) {
			Object listElementValue = arrayValuesOI.getListElement(arrayValues, i);
			if (listElementValue == null){
				values[i] = -1;
			}else{
				values[i] = PrimitiveObjectInspectorUtils.getInt(listElementValue, elementValueOI);
			}
			//LOG.error("value="+ values[i]);
		}

	}

	private void loadConditions(Object arrayConditions, String conditionsKey){
		
		currentConditions = cachedConditions.get(conditionsKey);
	
		if (currentConditions == null){
			currentConditions = new CachedConditions(arrayConditions, structConditionsElementOI, arrayConditionsOI, conditionNumberOI, arrayConditionsOI, conditionNumberOI);
			cachedConditions.put(conditionsKey, currentConditions);
		}
		
	}
	
	private void loadConditions(Object arrayConditions){
		currentConditions = new CachedConditions(arrayConditions, structConditionsElementOI, arrayConditionsOI, conditionNumberOI, arrayConditionsOI, conditionNumberOI);
	}
	
	@Override
	public String getDisplayString(String[] children) {
		return "MultiArrayMatch";
	}
	
	private static class CachedConditions {
		
		private transient int conditionsIds[];
		private transient int conditionsValues[][];
		
		public CachedConditions(Object arrayConditions, StructObjectInspector structConditionsElementOI, ListObjectInspector arrayConditionsOI, PrimitiveObjectInspector conditionNumberOI, ListObjectInspector arrayConditionsValuesOI, PrimitiveObjectInspector conditionValueOI){
			StructField conditionNumberField = structConditionsElementOI.getStructFieldRef("position");
			StructField conditionValuesField = structConditionsElementOI.getStructFieldRef("values");

			int arrayConditionsLength = arrayConditionsOI.getListLength(arrayConditions);
			conditionsIds = new int[arrayConditionsLength];
			conditionsValues = new int[arrayConditionsLength][];
			
			for (int i=0; i < arrayConditionsLength; i++) {
				Object listElementCondition = arrayConditionsOI.getListElement(arrayConditions, i);
				conditionsIds[i] = PrimitiveObjectInspectorUtils.getInt(structConditionsElementOI.getStructFieldData(listElementCondition, conditionNumberField), conditionNumberOI);
				//LOG.error("condition=" + conditionId);
				Object arrayConditionsValues = structConditionsElementOI.getStructFieldData(listElementCondition, conditionValuesField);
				int conditionSize = arrayConditionsValuesOI.getListLength(arrayConditionsValues);
				//LOG.error("size=" + conditionSize);
				conditionsValues[i] = new int[conditionSize];
				for (int j=0; j < conditionSize; j++) {
					Object listElementConditionValue = arrayConditionsValuesOI.getListElement(arrayConditionsValues, j);
					conditionsValues[i][j] = PrimitiveObjectInspectorUtils.getInt(listElementConditionValue, conditionValueOI);
					//LOG.error("condition_value["+ i + "]=" + conditionsValues[i][j]);
				}
				
			}
		}

		public boolean contains(int[] values) {
			boolean failToFind = false;
			//LOG.error("values=" + values.toString());
			//LOG.error("conditionsValues=" + conditionsValues.toString());
			for (int i=0; i<conditionsIds.length; i++){
				//LOG.error("i=" + i);
				//LOG.error("finding " + values[conditionsIds[i]] + " into " + conditionsValues[i]);
				if (!ArrayUtils.contains(conditionsValues[i], values[conditionsIds[i]])){
					//LOG.error("not found");
					failToFind = true;
					break;
				}else{
					//LOG.error("found");
				}
			}
			return !failToFind;
		}

	}	
	
}