package com.github.gbraccialli.hive.udf;

import java.util.ArrayList;
import java.util.Collections;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;


@Description(name = "BalanceFromRechargesAndOrders",
value = "_FUNC_( ...)  " +
		"  ")
public class BalanceFromRechargesAndOrders extends GenericUDF {
	
	static final Log LOG = LogFactory.getLog(BalanceFromRechargesAndOrders.class.getName());


	private ListObjectInspector arrayOI;
	private StructObjectInspector arrayElementOI;
	private PrimitiveObjectInspector keyOI;
	private PrimitiveObjectInspector dateOI;
	private PrimitiveObjectInspector valueOI;
	private String dateStructFieldName;
	private String valueStructFieldName;
	private String previousKey = "";
	private ArrayList<SortableRecord> credits;

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments)
			throws UDFArgumentException {


		boolean dateStructFieldFound = false;
		boolean valueStructFieldFound = false;
		
		arrayOI = (ListObjectInspector) arguments[0];
		arrayElementOI = (StructObjectInspector)arrayOI.getListElementObjectInspector();

		dateStructFieldName = ((ConstantObjectInspector)arguments[1]).getWritableConstantValue().toString();
		valueStructFieldName = ((ConstantObjectInspector)arguments[2]).getWritableConstantValue().toString();
		keyOI = (PrimitiveObjectInspector) arguments[3];
		dateOI = (PrimitiveObjectInspector) arguments[4];
		valueOI = (PrimitiveObjectInspector) arguments[5];

		ArrayList<ObjectInspector> foi = new ArrayList<ObjectInspector>();
		ArrayList<String> fname = new ArrayList<String>();

		//System.out.println("initialize");

		for (StructField field : arrayElementOI.getAllStructFieldRefs()) {
			fname.add(field.getFieldName());
			foi.add(field.getFieldObjectInspector());
			if (dateStructFieldName.equalsIgnoreCase(field.getFieldName())){
				dateStructFieldFound = true;
				//System.out.println("   DATE field = " + field.getFieldName());
			}else if (valueStructFieldName.equalsIgnoreCase(field.getFieldName())){
				valueStructFieldFound = true;
				//System.out.println("   VALUE field = " + field.getFieldName());
			}
			//System.out.println("field = " + field.getFieldName());
		}
		
		if (!dateStructFieldFound){
			throw new UDFArgumentException("Invalid dateStructFieldName on argument #2");
		}
		
		if (!valueStructFieldFound){
			throw new UDFArgumentException("Invalid valueStructFieldName on argument #3");
		}
			

		//fname.add("teste1");
		//fname.add("teste2");
		fname.add("split_value");
		fname.add("balance");

		foi.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
		foi.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
		//foi.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
		//foi.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);


		return ObjectInspectorFactory.getStandardListObjectInspector(
				ObjectInspectorFactory.getStandardStructObjectInspector(fname, foi) );    

	}

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {

		//LOG.error("evaluate guilherme");
		/*
		ArrayList record1 = new ArrayList(3);
		ArrayList record2 = new ArrayList(3);

		record1.add(new DoubleWritable(1.1));
		record1.add(new Text("record1 text"));
		record1.add(new DoubleWritable(1.3));

		record2.add(new DoubleWritable(2.1));
		record2.add(new Text("record2 text"));
		record2.add(new DoubleWritable(2.3));

		ArrayList returnObj = new ArrayList(2);

		returnObj.add(record1);
		returnObj.add(record2);

		return returnObj;
		 */

		/*
		List<?> list = arrayOI.getList(array);
	    if (list != null && !list.isEmpty()) {
	      for (Object row : list.toArray()) {
	    	  returnObj.add(row);
	      }
	    }
		 */
		
		/*
		 
 		int arrayLength = arrayOI.getListLength(array);
		double total = 0;
		
		for (int i=0; i < arrayLength; i++) {
			Object listElement = arrayOI.getListElement(array, i);
			ArrayList record = new ArrayList();
			if (listElement != null) {
				for (StructField field : arrayElementOI.getAllStructFieldRefs()){
					record.add(arrayElementOI.getStructFieldData(listElement, field));
				}
				StructField valueStructField = arrayElementOI.getStructFieldRef(valueStructFieldName);
				total += PrimitiveObjectInspectorUtils.getDouble(arrayElementOI.getStructFieldData(listElement, valueStructField), (PrimitiveObjectInspector)valueStructField.getFieldObjectInspector());
				record.add(new DoubleWritable(total));
				//record.add(arrayElementOI.getStructFieldData(listElement, arrayElementOI.getStructFieldRef(dateStructFieldName)));
				returnObj.add(record);
			}
		}


		 */

		Object array = arguments[0].get();
		String key = PrimitiveObjectInspectorUtils.getString(arguments[3].get(), keyOI);
		String timestamp = PrimitiveObjectInspectorUtils.getString(arguments[4].get(), dateOI);
		double value = PrimitiveObjectInspectorUtils.getDouble(arguments[5].get(), valueOI);
		double remainingValue = value;

		if (!previousKey.equals(key)){
			//LOG.error("new key" + key);
			loadCredits(array);
		}
		
		ArrayList returnObj = new ArrayList(2);
		
		if (credits.size() > 0){
			SortableRecord credit = credits.get(0);
			while (credit != null && timestamp.compareTo(credit.getTimestamp()) >= 0 && remainingValue > 0){
				if (remainingValue >= credit.getBalance()){
					double valueToDeduct  = credit.getBalance();
					credit.setBalance(credit.getBalance() - valueToDeduct);
					emitValues(valueToDeduct, credit.getBalance(), credit.getOriginalId(), returnObj, array);
					credits.remove(0);
					if (credits.size() > 0){
						credit = credits.get(0);
					}else{
						credit = null;
					}
					remainingValue -= valueToDeduct;
				}else{
					credit.setBalance(credit.getBalance() - remainingValue);
					emitValues(remainingValue, credit.getBalance(), credit.getOriginalId(), returnObj, array);
					remainingValue = 0;
				}
			}
		}
		
		if (remainingValue > 0){
			emitValues(remainingValue, remainingValue*-1, -1, returnObj, array);
		}

		previousKey = key;
		return returnObj;

	}

	private void emitValues(double value, double balance, int originalId, ArrayList returnObj, Object array) {
		
		ArrayList record = new ArrayList();
		for (StructField field : arrayElementOI.getAllStructFieldRefs()){
			if (originalId == -1){ 
				record.add(null);
			}else{
				Object listElement = arrayOI.getListElement(array, originalId);
				record.add(arrayElementOI.getStructFieldData(listElement, field));
			}
		}
		record.add(new DoubleWritable(value));
		record.add(new DoubleWritable(balance));
		returnObj.add(record);
		
	}

	private void loadCredits(Object array) {

		//LOG.error("loading credits...");
		
		if (array == null){
			credits = new ArrayList<SortableRecord>(0);
		}else{
		
			int arrayLength = arrayOI.getListLength(array);
			credits = new ArrayList<SortableRecord>(arrayLength);
			
			for (int i=0; i < arrayLength; i++) {
				Object listElement = arrayOI.getListElement(array, i);
				
				StructField dateStructField = arrayElementOI.getStructFieldRef(dateStructFieldName);
				StructField valueStructField = arrayElementOI.getStructFieldRef(valueStructFieldName);
				
				String timestamp = PrimitiveObjectInspectorUtils.getString(arrayElementOI.getStructFieldData(listElement, dateStructField), (PrimitiveObjectInspector)dateStructField.getFieldObjectInspector()); 
				double value = PrimitiveObjectInspectorUtils.getDouble(arrayElementOI.getStructFieldData(listElement, valueStructField), (PrimitiveObjectInspector)valueStructField.getFieldObjectInspector());
				
				credits.add(new SortableRecord(timestamp, value ,i));
				//LOG.error("credit:" + timestamp + " - " + value);
			}
			
			Collections.sort(credits);
	
			//LOG.error("first" + credits.get(0).getTimestamp() + " = " + credits.get(0).getBalance());
		}
	}

	@Override
	public String getDisplayString(String[] children) {
		return "BalanceFromRechargesAndOrders";
	}
	
	private static class SortableRecord implements Comparable{
		
		private String timestamp;
		private double balance;
		private int originalId;
		
		public int getOriginalId() {
			return originalId;
		}


		public void setOriginalId(int originalId) {
			this.originalId = originalId;
		}


		public SortableRecord(String timestamp, double balance, int originalId) {
			super();
			this.timestamp = timestamp;
			this.balance = balance;
			this.originalId = originalId;
		}
		
		
		public String getTimestamp() {
			return timestamp;
		}


		public void setTimestamp(String timestamp) {
			this.timestamp = timestamp;
		}


		public double getBalance() {
			return balance;
		}


		public void setBalance(double balance) {
			this.balance = balance;
		}



		@Override
		public int compareTo(Object o) {
			return (this.getTimestamp().compareTo(((SortableRecord)o).getTimestamp()));
		}
		
		
	}
}