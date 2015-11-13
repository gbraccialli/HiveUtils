package com.github.gbraccialli.hive.serde;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

@SerDeSpec(schemaProps = {
		serdeConstants.LIST_COLUMNS, serdeConstants.LIST_COLUMN_TYPES,
		NKeys_MapKeyValue.DELIMITER, })
public class NKeys_MapKeyValue extends AbstractSerDe {

	public static final Logger LOG = LoggerFactory.getLogger(NKeys_MapKeyValue.class.getName());

	public static final String DELIMITER = "delimiter";

	int numColumns;
	String delimiter;

	Pattern inputPattern;

	StructObjectInspector rowOI;
	List<Object> row;
	List<TypeInfo> columnTypes;
	Object[] outputFields;
	Text outputRowText;

	boolean alreadyLoggedNoMatch = false;
	boolean alreadyLoggedPartialMatch = false;

	@Override
	public void initialize(Configuration conf, Properties tbl)
			throws SerDeException {

		// We can get the table definition from tbl.

		// Read the configuration parameters
		delimiter = tbl.getProperty(DELIMITER);
		String columnNameProperty = tbl.getProperty(serdeConstants.LIST_COLUMNS);
		String columnTypeProperty = tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES);


		List<String> columnNames = Arrays.asList(columnNameProperty.split(","));
		columnTypes = TypeInfoUtils
				.getTypeInfosFromTypeString(columnTypeProperty);
		assert columnNames.size() == columnTypes.size();
		numColumns = columnNames.size();

		List<ObjectInspector> columnOIs = new ArrayList<ObjectInspector>(columnNames.size());
		for (int c = 0; c < numColumns; c++) {
			TypeInfo typeInfo = columnTypes.get(c);

			if (c == numColumns-1){
				//TODO this version consider last column will be a map<string,string>, need to add code to validate that and also to allow map<any-primitive-type,any-primitive-type>
				StandardMapObjectInspector oi =
						ObjectInspectorFactory.getStandardMapObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector,PrimitiveObjectInspectorFactory.javaStringObjectInspector);
				columnOIs.add(oi);

			}else{
				if (typeInfo instanceof PrimitiveTypeInfo) {
					PrimitiveTypeInfo pti = (PrimitiveTypeInfo) columnTypes.get(c);
					AbstractPrimitiveJavaObjectInspector oi =
							PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(pti);
					columnOIs.add(oi);
				} else {
					throw new SerDeException(getClass().getName()
							+ " doesn't allow column [" + c + "] named "
							+ columnNames.get(c) + " with type " + columnTypes.get(c));
				}    	  
			}
		}

		// StandardStruct uses ArrayList to store the row.
		rowOI = ObjectInspectorFactory.getStandardStructObjectInspector(
				columnNames,columnOIs,Lists.newArrayList(Splitter.on('\0').split(tbl.getProperty("columns.comments"))));

		row = new ArrayList<Object>(numColumns);
		// Constructing the row object, etc, which will be reused for all rows.
		for (int c = 0; c < numColumns; c++) {
			row.add(null);
		}
		outputFields = new Object[numColumns];
		outputRowText = new Text();
	}

	@Override
	public ObjectInspector getObjectInspector() throws SerDeException {
		return rowOI;
	}

	@Override
	public Class<? extends Writable> getSerializedClass() {
		return Text.class;
	}


	@Override
	public Object deserialize(Writable blob) throws SerDeException {

		Text rowText = (Text) blob;
		String columns[] = rowText.toString().split(Pattern.quote(delimiter));

		for (int keyId = 0; keyId < numColumns; keyId++) {
			//TODO this version consider last column will be a map<string,string>, need to add code to validate that and also to allow map<any-primitive-type,any-primitive-type>
			if (keyId == numColumns-1){
				Map<Object, Object> map = new LinkedHashMap<Object, Object>();
				for (int i = keyId; i < columns.length; i++){
					String key = columns[i];
					i++;
					String value = "";
					if (columns.length > i){
						value = columns[i];
					}
					map.put(key, value);
				}
				row.set(keyId, map);
			}else{
				try {
					TypeInfo typeInfo = columnTypes.get(keyId);

					// Convert the column to the correct type when needed and set in row obj
					PrimitiveTypeInfo pti = (PrimitiveTypeInfo) typeInfo;
					switch (pti.getPrimitiveCategory()) {
					case STRING:
						row.set(keyId, columns[keyId]);
						break;
					case BYTE:
						Byte b;
						b = Byte.valueOf(columns[keyId]);
						row.set(keyId,b);
						break;
					case SHORT:
						Short s;
						s = Short.valueOf(columns[keyId]);
						row.set(keyId,s);
						break;
					case INT:
						Integer i;
						i = Integer.valueOf(columns[keyId]);
						row.set(keyId, i);
						break;
					case LONG:
						Long l;
						l = Long.valueOf(columns[keyId]);
						row.set(keyId, l);
						break;
					case FLOAT:
						Float f;
						f = Float.valueOf(columns[keyId]);
						row.set(keyId,f);
						break;
					case DOUBLE:
						Double d;
						d = Double.valueOf(columns[keyId]);
						row.set(keyId,d);
						break;
					case BOOLEAN:
						Boolean bool;
						bool = Boolean.valueOf(columns[keyId]);
						row.set(keyId, bool);
						break;
					case TIMESTAMP:
						Timestamp ts;
						ts = Timestamp.valueOf(columns[keyId]);
						row.set(keyId, ts);
						break;
					case DATE:
						Date date;
						date = Date.valueOf(columns[keyId]);
						row.set(keyId, date);
						break;
					case DECIMAL:
						HiveDecimal bd = HiveDecimal.create(columns[keyId]);
						row.set(keyId, bd);
						break;
					case CHAR:
						HiveChar hc = new HiveChar(columns[keyId], ((CharTypeInfo) typeInfo).getLength());
						row.set(keyId, hc);
						break;
					case VARCHAR:
						HiveVarchar hv = new HiveVarchar(columns[keyId], ((VarcharTypeInfo)typeInfo).getLength());
						row.set(keyId, hv);
						break;
					default:
						throw new SerDeException("Unsupported type " + typeInfo);
					}
				} catch (RuntimeException e) {
					row.set(keyId, null);
				}
			}
		}
		return row;
	}

	@Override
	public Writable serialize(Object obj, ObjectInspector objInspector)
			throws SerDeException {
		throw new UnsupportedOperationException(
				"Regex SerDe doesn't support the serialize() method");
	}

	@Override
	public SerDeStats getSerDeStats() {
		// no support for statistics
		return null;
	}
}