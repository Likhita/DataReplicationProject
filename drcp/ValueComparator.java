package org.edg.data.replication.drcp;

import java.util.Comparator;
import java.util.Map;

public class ValueComparator implements Comparator<Map.Entry<String,Integer>> {

	public int compare(Map.Entry<String,Integer> obj1, Map.Entry<String,Integer> obj2){
		Map.Entry<String,Integer> e1 = obj1 ;
		Map.Entry<String,Integer> e2 = obj2 ;//Sort based on values.
		Integer value1 = e1.getValue();
		Integer value2 = e2.getValue();
		return value2.compareTo( value1 );
		}
}
