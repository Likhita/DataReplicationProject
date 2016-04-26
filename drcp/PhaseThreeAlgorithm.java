package org.edg.data.replication.drcp;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class PhaseThreeAlgorithm {

	private static PhaseThreeAlgorithm instance = null;
	
	public static PhaseThreeAlgorithm getInstance(){
		if( instance == null)
			instance = new PhaseThreeAlgorithm();
		return instance;
	}
	
	private PhaseThreeAlgorithm(){		
	}
	
	/**
	 * hitsTable can be hitsPerClusterTable or hitsPerSiteTable <siteId/cid, #replicas>
	 * @param totalNumber
	 * @param hitsTable
	 * @return
	 */
	public Hashtable<String,Double> getNumberOfReplicas(double totalNumber, Hashtable<String, Integer> hitsTable) {
		Hashtable<String,Double> replicasNumberTable=new Hashtable<String,Double>();
		double sum=0.0, num;
		Iterator<String> itr=hitsTable.keySet().iterator();
		while(itr.hasNext()){
			String key = itr.next();
			sum += hitsTable.get(key);
		}
		itr=hitsTable.keySet().iterator();
		while(itr.hasNext()){
			String key = itr.next();
			num=Math.round(Math.ceil((totalNumber-1)*(hitsTable.get(key)/sum)));
			replicasNumberTable.put(key,num);
		}
		return replicasNumberTable;
	}
	
	public List<Map.Entry<String,Integer>>  sortClusters(Hashtable<String, Integer> histPerCluster) {		
		List<Map.Entry<String,Integer>> sortedList = new ArrayList<Map.Entry<String,Integer>>(histPerCluster.entrySet());
		Collections.sort(sortedList, new ValueComparator());
		return sortedList; 
	}
}
