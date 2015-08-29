package com.github.gbraccialli.hive.udaf;
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import jodd.util.collection.SortedArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

@Description(name = "collect_list", value = "_FUNC_(x) - Returns a list of objects with duplicates")
public class DedupTimestampDurationHiveUDAF extends AbstractGenericUDAFResolver {

	static final Log LOG = LogFactory.getLog(DedupTimestampDurationHiveUDAF.class.getName());
	
	//TODO make it configurable (constants from udaf call)
	static final long TIMESTAMP_THRESHOLD = 30;
	static final long DURATION_THRESHOLD = 15;
	

	public DedupTimestampDurationHiveUDAF() {
	}

	@Override
	public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
			throws SemanticException {
		if (parameters.length != 4) {
			throw new UDFArgumentTypeException(parameters.length - 1,
					"Exactly 4 arguments are expected.");
		}
		return new GenericUDAFMkCollectionEvaluator_test();
	}

	
	private static class SortableRecord implements Comparable{
		
		private long timestamp;
		private int duration;
		private String type;
		private Object struct;
		
		public SortableRecord(long timestamp, int duration, String type,
				Object struct) {
			super();
			this.timestamp = timestamp;
			this.duration = duration;
			this.type = type;
			this.struct = struct;
		}
		
		
		public long getTimestamp() {
			return timestamp;
		}
		public void setTimestamp(long timestamp) {
			this.timestamp = timestamp;
		}
		public int getDuration() {
			return duration;
		}
		public void setDuration(int duration) {
			this.duration = duration;
		}
		public String getType() {
			return type;
		}
		public void setType(String type) {
			this.type = type;
		}
		public Object getStruct() {
			return struct;
		}
		public void setStruct(Object struct) {
			this.struct = struct;
		}
		
		@Override
		public int compareTo(Object o) {
			return (int)(this.getTimestamp() - ((SortableRecord)o).getTimestamp());
		}
		
		
	}
	
	public static class GenericUDAFMkCollectionEvaluator_test extends GenericUDAFEvaluator {


		private static final long serialVersionUID = 1l;


		// For PARTIAL1 and COMPLETE: ObjectInspectors for original data
		private transient ObjectInspector structOI;
		private transient PrimitiveObjectInspector timestampOI;
		private transient PrimitiveObjectInspector durationOI;
		private transient PrimitiveObjectInspector typeOI;

		// For PARTIAL2 and FINAL: ObjectInspectors for partial aggregations (list
		// of objs)
		private transient StandardListObjectInspector loi;
		private transient ListObjectInspector internalMergeOI;


		@Override
		public ObjectInspector init(Mode m, ObjectInspector[] parameters)
				throws HiveException {
			super.init(m, parameters);
			
			//System.out.println("debug=on");
			//LOG.error("debug=on");
			// init output object inspectors
			// The output of a partial aggregation is a list
			if (m == Mode.PARTIAL1) {
				//TODO only hive.map.aggr=false supported, TODO implement map aggregation
				//not implemented yet
				throw new HiveException("you must define set hive.map.aggr=false; to use this UDAF");
				/*
    			inputOI = (ObjectInspector) parameters[0];
    			return ObjectInspectorFactory
        			.getStandardListObjectInspector((ObjectInspector) ObjectInspectorUtils
            		.getStandardObjectInspector(inputOI));
				 */
			} else {
				if (!(parameters[0] instanceof ListObjectInspector)) {
					//COMPLETE.

					timestampOI = (PrimitiveObjectInspector)  parameters[0];
					durationOI = (PrimitiveObjectInspector)  parameters[1];
					typeOI = (PrimitiveObjectInspector)  parameters[2];
					structOI = (ObjectInspector)  parameters[3];
					
                    return ObjectInspectorFactory.getStandardListObjectInspector(
                    		(ObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(structOI));
                    
				} else {
					//TODO only hive.map.aggr=false supported, TODO implement map aggregation
					//not implemented yet
					throw new HiveException("you must define set hive.map.aggr=false; to use this UDAF");
					/*  
      					internalMergeOI = (ListObjectInspector) parameters[0];
      					inputOI = (ObjectInspector) internalMergeOI.getListElementObjectInspector();
      					loi = (StandardListObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(internalMergeOI);
      					return loi;
					 */
				}
			}
		}

		class MkArrayAggregationBuffer extends AbstractAggregationBuffer {

			private ArrayList<SortableRecord> container;

			public MkArrayAggregationBuffer() {
				container = new ArrayList<SortableRecord>();
			}
		}

		@Override
		public void reset(AggregationBuffer agg) throws HiveException {
			((MkArrayAggregationBuffer) agg).container.clear();
		}

		@Override
		public AggregationBuffer getNewAggregationBuffer() throws HiveException {
			MkArrayAggregationBuffer ret = new MkArrayAggregationBuffer();
			reset(ret);
			return ret;
		}

		//mapside
		@Override
		public void iterate(AggregationBuffer agg, Object[] parameters)
				throws HiveException {
			
			assert (parameters.length == 4);
			
			if (parameters[0] != null){
				Long timestamp = PrimitiveObjectInspectorUtils.getLong(parameters[0], timestampOI);
				Integer duration = PrimitiveObjectInspectorUtils.getInt(parameters[1], durationOI);
				String type = PrimitiveObjectInspectorUtils.getString(parameters[2], typeOI);
				Object struct = ObjectInspectorUtils.copyToStandardObject(parameters[3], this.structOI);;
	
				if (timestamp != null && struct != null) {
					
					if (duration == null){
					 	duration = 0;
					}
					
					if (type == null){
						type = "";
					}
					
					SortableRecord record = new SortableRecord(timestamp, duration, type, struct);
					MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer) agg;
					putIntoCollection(record, myagg);
					
				}
			}
		}

		//mapside
		@Override
		public Object terminatePartial(AggregationBuffer agg) throws HiveException {

			//TODO only hive.map.aggr=false supported, TODO implement map aggregation
			//not implemented yet
			return null;
			
			/*
			MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer) agg;
			List<Object> ret = new ArrayList<Object>(myagg.container.size());
			ret.addAll(myagg.container);
			return ret;
			*/
			

			
		}

		@Override
		public void merge(AggregationBuffer agg, Object partial)
				throws HiveException {
			
			//TODO only hive.map.aggr=false supported, TODO implement map aggregation
			//not implemented yet

			return;
			
			/*
			MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer) agg;
			List<Object> partialResult = (ArrayList<Object>) internalMergeOI.getList(partial);
			if (partialResult != null) {
				for(Object i : partialResult) {
					putIntoCollection(i, myagg);
				}
			}
			*/
		}

		@Override
		public Object terminate(AggregationBuffer agg) throws HiveException {

			SortableRecord chosenDedupElement = null;
			MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer) agg;
			
			Collections.sort(myagg.container);
			
			List<Object> ret = new ArrayList<Object>(myagg.container.size()/12); //average repeated rows
			if (myagg.container.size() > 0){
				chosenDedupElement = myagg.container.get(0);
			}
			for (SortableRecord record : myagg.container){
				if ((record.timestamp - chosenDedupElement.timestamp) <= TIMESTAMP_THRESHOLD && Math.abs(record.duration - chosenDedupElement.duration) <= DURATION_THRESHOLD){
					//same element, check type to choose which element to keep
					if (record.getType().compareTo(chosenDedupElement.getType()) > 0){
						chosenDedupElement.setStruct(record.getStruct());
						chosenDedupElement.setType(record.getType());
						//LOG.error("changed: was " + chosenDedupElement.getType() + " now is:" + record.getType() + " compare=" + record.getType().compareTo(chosenDedupElement.getType()));
					}else{
						//LOG.error("not changedi: was " + chosenDedupElement.getType() + " new (ignored):" + record.getType() + " compare=" + record.getType().compareTo(chosenDedupElement.getType()));
					}
				}else{
					ret.add(chosenDedupElement.getStruct());
					chosenDedupElement = record;
				}
			}
			if (chosenDedupElement != null){
				ret.add(chosenDedupElement.getStruct());
			}
			return ret;
		}

		private void putIntoCollection(SortableRecord p, MkArrayAggregationBuffer myagg) {
			myagg.container.add(p);
		}

	}
}	
