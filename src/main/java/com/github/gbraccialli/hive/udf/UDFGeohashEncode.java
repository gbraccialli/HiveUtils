package com.github.gbraccialli.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import com.github.davidmoten.geo.GeoHash;

public class UDFGeohashEncode extends UDF {
	
	public Text evaluate(DoubleWritable latitude, DoubleWritable longitude, IntWritable precision) {

		if (latitude == null || longitude == null || precision == null) {
			return null;
		}

		return new Text(GeoHash.encodeHash(latitude.get(), longitude.get(), precision.get()));

	}
	
}
