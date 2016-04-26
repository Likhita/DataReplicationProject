package org.edg.data.replication.drcp;

import java.util.Comparator;
import java.util.Map;

public class DoubleValueComparator implements Comparator<Map.Entry<String,Double>> {

	public int compare(Map.Entry<String,Double> obj1, Map.Entry<String,Double> obj2){
		Map.Entry<String,Double> e1 = obj1 ;
		Map.Entry<String,Double> e2 = obj2 ;//Sort based on values.
		Double value1 = e1.getValue();
		Double value2 = e2.getValue();
		return value2.compareTo( value1 );
		}
}
