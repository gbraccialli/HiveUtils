package com.github.gbraccialli.hive.udf;

import java.util.ArrayList;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;


@Description(name = "ArrayFilterIndexesLike",
value = "_FUNC_( ...)  " +
		"  ")
public class ArrayFilterIndexesLike extends GenericUDF {

	static final Log LOG = LogFactory.getLog(ArrayFilterIndexesLike.class.getName());

	private ListObjectInspector arrayValuesOI;
	private PrimitiveObjectInspector elementValueOI;

	private PrimitiveObjectInspector conditionOI;

	private boolean useCache = false;
	private boolean loaded = false;

	private String[] values;

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments)
			throws UDFArgumentException {

		arrayValuesOI = (ListObjectInspector) arguments[0];
		elementValueOI = (PrimitiveObjectInspector)arrayValuesOI.getListElementObjectInspector();

		conditionOI = (PrimitiveObjectInspector) arguments[1];

		if (arguments.length > 2) {
			ObjectInspector cachedOI = arguments[2];
			if (!ObjectInspectorUtils.isConstantObjectInspector(cachedOI)
					|| (cachedOI.getCategory() != ObjectInspector.Category.PRIMITIVE)
					|| ((PrimitiveObjectInspector) cachedOI).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.BOOLEAN ) {
				throw new UDFArgumentTypeException(1, "Last argument 'useCache' must be a constant boolean value "
						+ cachedOI.getTypeName() + " was passed as parameter.");
			}
			Object o = ((ConstantObjectInspector) cachedOI).getWritableConstantValue();
			useCache = ((BooleanWritable) o).get();
		}

		return ObjectInspectorFactory.getStandardListObjectInspector(
				PrimitiveObjectInspectorFactory.writableIntObjectInspector
				);

	}

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {

		ArrayList returnIndexes = new ArrayList();
		if (arguments[0] == null || arguments[1] == null)
			return returnIndexes;

		Object conditionObj = arguments[1].get();

		if (conditionObj == null)
			return returnIndexes;

		if (!useCache || !loaded)
			loadValues(arguments[0].get());

		String condition = PrimitiveObjectInspectorUtils.getString(conditionObj, conditionOI);

		Pattern pattern = Pattern.compile(likePatternToRegExp(condition));

		for (int i=0; i < values.length; i++){
			if (pattern.matcher(values[i]).matches())
				returnIndexes.add(new IntWritable(i));
		}

		return returnIndexes;
	}


	private void loadValues(Object arrayValues){

		if (arrayValues == null){
			values = new String[0];
		}else{
			int arrayValuesLength = arrayValuesOI.getListLength(arrayValues);
			values = new String[arrayValuesLength];

			for (int i=0; i < arrayValuesLength; i++) {
				Object listElementValue = arrayValuesOI.getListElement(arrayValues, i);
				if (listElementValue == null){
					values[i] = "";
				}else{
					values[i] = PrimitiveObjectInspectorUtils.getString(listElementValue, elementValueOI);
				}
			}
		}
		loaded = true;

	}

	public static String likePatternToRegExp(String likePattern) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < likePattern.length(); i++) {
			// Make a special case for "\\_" and "\\%"
			char n = likePattern.charAt(i);
			if (n == '\\'
					&& i + 1 < likePattern.length()
					&& (likePattern.charAt(i + 1) == '_' || likePattern.charAt(i + 1) == '%')) {
				sb.append(likePattern.charAt(i + 1));
				i++;
				continue;
			}

			if (n == '_') {
				sb.append(".");
			} else if (n == '%') {
				sb.append(".*");
			} else {
				sb.append(Pattern.quote(Character.toString(n)));
			}
		}
		return sb.toString();
	}

	@Override
	public String getDisplayString(String[] children) {
		return "ArrayFilterIndexesLike";
	}


}