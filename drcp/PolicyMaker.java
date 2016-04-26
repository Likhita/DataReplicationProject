package org.edg.data.replication.drcp;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Vector;

import org.edg.data.replication.optorsim.infrastructure.GridContainer;
import org.edg.data.replication.optorsim.infrastructure.GridSite;

public class PolicyMaker {

	//maintains reference to the cluster heads
	// HashTable - (fileID, number of hits )

	// ----loop for every interval---
	//collects info from cluster heads 
	//---------------------aggregate the collected info----------------------
	/*
	1.	The Region master calculates AF(p) in each region after getting the information for popular file from region headers. 
	2.	Then it sorts its records in descending order based on the AF(p). 	
	---------------------------------------------------------------------
	 */	
	//call optimizer and pass the hashtable 
	//sleep for 30 seconds
	//----end loop---

	//TODO change this time interval
	public static final int TIME_INTERVAL = 100; // 30 seconds
	private List<ClusterHead> listOfClusters = new ArrayList<ClusterHead>();

	// timeIntervalTable contains aggregated hits per file table for each cycle
	Vector<Hashtable<String,Integer>> timeIntervalTable= new Vector<Hashtable<String,Integer>>();
	// phaseThreeHitsTable contains #hits per cluster for a popular file for each cycle
	Vector<Hashtable<String,Integer>> phaseThreeHitsTable= new Vector<Hashtable<String,Integer>>();
	private static PolicyMaker instance = null;
	private boolean execute = false;
	private PhaseThreeAlgorithm phaseThree=PhaseThreeAlgorithm.getInstance();

	public static PolicyMaker getInstance(){
		if( instance == null)
			instance = new PolicyMaker();
		return instance;
	}

	private PolicyMaker(){
		populateClusterList();
	}

	public void populateClusterList(){
		listOfClusters.add(new ClusterHead("C1"));
		listOfClusters.add(new ClusterHead("C2"));
		listOfClusters.add(new ClusterHead("C3"));
	}

	public boolean executeStatus() {
		return execute;
	}

	public HashMap<String, List<String>> startPM(){
		execute = true;
		HashMap<String, List<String>> resultFiles = new HashMap<String, List<String>>();
		String lfn1, lfn2;
		List<String> list1, list2;
		try {

			//beginning of each cycle
			//1. ask all the headers for total hits per file.
			Hashtable<String, Integer> aggregatedHitsPerFile = createPMAggregateTable();
			//2. add this aggregatedHitsPerFile into timeIntervalTable Vector
			addToTimeIntervalTable(aggregatedHitsPerFile);
			//3. create access frequency table for all tables in timeIntervalTable
			Hashtable<String,Double> afTable=createAccessFrequencyTable();
			//4. get the popular file based on access frequency table
			String[] popularFiles= new String[2];
			//sort the popular files
			List<Entry<String, Double>> sortedFileList = sortClusters(afTable);	
			if(sortedFileList.size() > 0) {
				popularFiles[0]= sortedFileList.get(0).getKey();
				System.out.println("popular file: " + popularFiles[0]);
				//5. compute total #replicas required for a popular file
				double totalReplicas=Math.round(findTotalNumberOfReplicas(popularFiles[0], afTable));
				System.out.println("totalReplicas: " + totalReplicas);
				//6. compute #hits per cluster table for a popular file -- repeat
				Hashtable<String, Integer> hitsPerCluster = createClusterPopularFileTable(popularFiles[0]);
				Set<String> keys = hitsPerCluster.keySet();
				System.out.println("Aggregated cluster info of size:" +hitsPerCluster.size() );
				for(String key: keys){
					System.out.println("clusterId: " + key + " #hits: " + hitsPerCluster.get(key)); 
				}
				//7. sort hitsPerCluster to identify the cluster with max #hits for replication -- repeat
				PhaseThreeAlgorithm phaseThree=PhaseThreeAlgorithm.getInstance();
				List<Map.Entry<String,Integer>> sortedClusters = phaseThree.sortClusters(hitsPerCluster); 
				//8. get the number of replicas needed for that cluster
				Hashtable<String, Double> replicaCountPerCluster = phaseThree.getNumberOfReplicas(totalReplicas, hitsPerCluster);
				Set<String> keys2 = replicaCountPerCluster.keySet();
				System.out.println("replicaCountPerCluster size:" + replicaCountPerCluster.size());
				for(String key: keys2){
					System.out.println("clusterId: " + key + " rep count: " + replicaCountPerCluster.get(key)); 
				}					

				lfn1 = popularFiles[0];
				list1 = startReplicating(replicaCountPerCluster,sortedClusters,totalReplicas, popularFiles[0]);
				resultFiles.put(lfn1, list1);
				// 10. get second popular file
				popularFiles[1]= sortedFileList.get(1).getKey();
				Hashtable<String, Integer> hitsPerCluster2 = createClusterPopularFileTable(popularFiles[1]);
				if(hitsPerCluster2.size() > 0 ) {
					System.out.println("2nd file replication");
				//sort hitsPerCluster to identify the cluster with max #hits for replication -- repeat
					List<Map.Entry<String,Integer>> sortedClusters2 = phaseThree.sortClusters(hitsPerCluster2);
					list2 = replicatePF2(popularFiles[1], sortedClusters2.get(0).getKey());
					lfn2 = popularFiles[1];
					resultFiles.put(lfn2, list2);
				}
			}
		}  catch (Exception e) {
			e.printStackTrace();
		} finally {
			execute = false;
		}
		return resultFiles;
	}

	public List<Map.Entry<String,Double>>  sortClusters(Hashtable<String, Double> afTable) {		
		List<Map.Entry<String,Double>> sortedList = new ArrayList<Map.Entry<String,Double>>(afTable.entrySet());
		Collections.sort(sortedList, new DoubleValueComparator());
		return sortedList; 
	}

	// Note- the size of clusterHeadList is same as the size of sortedClusters
	private List<String> startReplicating(Hashtable<String, Double> replicaCountPerCluster, 
			List<Map.Entry<String,Integer>> sortedClusters, double totalReplicas, String popularFile){
		String cid;
		Double replicas, toReplicate;
		List<String> infoResult = new ArrayList<String>();
		for(int i=0;i<sortedClusters.size();i++){
			cid = sortedClusters.get(i).getKey();
			replicas = replicaCountPerCluster.get(cid);
			toReplicate = totalReplicas - replicas;	
			System.out.println("cid:"+cid +" replicas:"+replicas +" to repl:"+ toReplicate);
			if(toReplicate > 0) {
				infoResult.add(replicatePF1(toReplicate, popularFile, cid));	
				// TODO: confirm this below step and if condition
				totalReplicas = toReplicate;
			}
		}
		return infoResult;
	}

	private String replicatePF1(Double toReplicate, String popularFile, String cid) {
		Hashtable<String,Integer> hitsPerSite = getAllHitsPerSite(cid);
		Hashtable<String, Double> replicaCountPerSite =phaseThree.getNumberOfReplicas(toReplicate, hitsPerSite);
		// now sort these sites to identify destination
		List<Map.Entry<String,Integer>> sortedSites = phaseThree.sortClusters(hitsPerSite);
		String siteId = sortedSites.get(0).getKey();				
		Double count = replicaCountPerSite.get(siteId);
		System.out.println(siteId + "_" + count);
		return siteId + "_" + count;
		//replicate(popularFile, count, siteId);
	}

	private List<String> replicatePF2(String popularFile, String cid){
		List<String> infoList = new ArrayList<String>();
		Hashtable<String,Integer> hitsPerSite = getAllHitsPerSite(cid);
		List<Map.Entry<String,Integer>> sortedSites = phaseThree.sortClusters(hitsPerSite);
		String siteId = sortedSites.get(0).getKey();
		infoList.add(siteId + "_" + 1);
		return infoList;
		//replicate(popularFile, 1, siteId);
	}

	private void replicate(String popularFile, double count, String siteId) {
		int id = Integer.parseInt(siteId);
		GridSite requestedSite = GridContainer.getInstance().findGridSiteByID(id);
		System.out.println("Calling replicate for lfn:" + popularFile + " count: " + 
				count + " destination site: " + siteId + " cluser id : " + requestedSite.getClusterId());
		DRCPOptimiser drcpOptor = new DRCPOptimiser(requestedSite);
		//drcpOptor.replicateFiles(count, popularFile);
	}

	private Hashtable<String,Integer> getAllHitsPerSite(String cid) {
		//TODO: confirm this if we need to use cluster id
		Hashtable<String,Integer> allHitsPerSite = new Hashtable<String, Integer>();
		for(int i = 0; i < listOfClusters.size(); i ++) 
			allHitsPerSite.putAll(listOfClusters.get(i).getHitsPerSiteTable());
		/*ClusterHead cHead = getMatchingClusterHead(cid);
		return cHead.getHitsPerSiteTable();*/
		return allHitsPerSite;
	}

	private ClusterHead getMatchingClusterHead(String cid) {
		ClusterHead cHead = null;
		for(int i = 0; i < listOfClusters.size(); i++) {
			cHead = listOfClusters.get(i);
			if(cHead.getClusterId().equals(cid))
				return cHead;
		}
		return null;
	}

	private String getDestinationSite() {
		return null;
	}

	//phase 1- aggregate the data collected from cluster Heads 
	/**
	 * Aggregate the data collected from cluster heads
	 * @return
	 */
	private Hashtable<String,Integer> createPMAggregateTable(){
		Hashtable<String,Integer> pmAggregateTable= new Hashtable<String,Integer>();
		Hashtable<String,Integer> chSentTable=new Hashtable<String,Integer>();
		for(int i=0;i<listOfClusters.size();i++){
			chSentTable=listOfClusters.get(i).getAggregateInfo();
			Set<String> keys = chSentTable.keySet();
			for(String key: keys){
				if(!pmAggregateTable.containsKey(key))
					pmAggregateTable.put(key,chSentTable.get(key));
				else
					pmAggregateTable.put(key,pmAggregateTable.get(key)+chSentTable.get(key));
			}
		}
		return pmAggregateTable;
	}

	// add aggregated table to time interval Vector for each cycle
	private void addToTimeIntervalTable(Hashtable<String,Integer> tablePerCycle){
		timeIntervalTable.add(tablePerCycle);
		System.out.println("interval:" + timeIntervalTable.size() 
				+ " size of tablePerCycle: " + tablePerCycle.size());
	}

	/**
	 * calculate access frequency for each file using the aggregated hits per file table 
	 * taking into account the weights corresponding to each interval
	 * @return accessFrequencyTable<lfn,AF(lfn)>
	 */
	private Hashtable<String,Double> createAccessFrequencyTable(){
		double accessFrequency=0;
		Hashtable<String,Integer> intervalTable= new Hashtable<String,Integer>();
		Hashtable<String,Double> accessFrequencyTable= new Hashtable<String,Double>();
		for(int i=timeIntervalTable.size() -1, j=1;i>=0;i--,j++){
			intervalTable=timeIntervalTable.get(i);
			Set<String> keys = intervalTable.keySet();
			for(String key: keys){
				accessFrequency=intervalTable.get(key)*(1/Math.pow(2,(j-1)));
				if(!accessFrequencyTable.containsKey(key))
					accessFrequencyTable.put(key,accessFrequency);
				else
					accessFrequencyTable.put(key,accessFrequencyTable.get(key)+accessFrequency);
			}
		}
		return accessFrequencyTable;	
	}

	//Phase 2- returns the total number of replicas required for the popular file
	private Double findTotalNumberOfReplicas(String popularFileKey, Hashtable<String,Double> afTable){
		Double AFsum=0.0;
		double AFpopular = afTable.get(popularFileKey); //AF(p)
		double avgAFPopular=AFpopular/timeIntervalTable.size();
		for(Double value: afTable.values()){
			AFsum+=value;
		}
		//TODO: confirm this computation
		double avgAFAllFiles=AFsum/(afTable.size()*timeIntervalTable.size());
		return Math.ceil(avgAFPopular/avgAFAllFiles);
	}

	/**
	 * Phase 3 - Policy maker requests cluster heads to send info about the popular file
	 * aggregate the number of hits for each cluster
	 * @param popularFile
	 * @return clusterPopularFileTable<clusterId,number of hits>
	 */
	private Hashtable<String,Integer> createClusterPopularFileTable(String popularFile){
		//<key:cluster id, value:# hits>
		Hashtable<String,Integer> clusterPopularFileTable= new Hashtable<String,Integer>();
		Hashtable<String,Integer> fromClusterHeader=new Hashtable<String,Integer>();
		for(int i=0;i<listOfClusters.size();i++){
			fromClusterHeader=listOfClusters.get(i).getHistPerCluster(popularFile);
			System.out.println("size of fromClusterHeader:" + fromClusterHeader.size());
			Set<String> keys = fromClusterHeader.keySet();
			for(String key: keys){
				if(!clusterPopularFileTable.containsKey(key))
					clusterPopularFileTable.put(key,fromClusterHeader.get(key));
				else
					clusterPopularFileTable.put(key,clusterPopularFileTable.get(key)+fromClusterHeader.get(key));
			}
		}
		return clusterPopularFileTable;
	}
}
