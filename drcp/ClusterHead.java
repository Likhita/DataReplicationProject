package org.edg.data.replication.drcp;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Set;


import org.edg.data.replication.optorsim.infrastructure.GridContainer;
import org.edg.data.replication.optorsim.infrastructure.GridSite;

public class ClusterHead {
	//hash table (fileID, time requested, cluster ID, number of hits)

	//contact grid container to get file info of all grid sites
	// send hashtable when requested by region Master
	private String clusterId;
	public static final String KEY_SEPARATOR = ":";

	// Key<String>="lfn:siteId", Value=<SiteRecord>
	private Hashtable<String, SiteRecord> fileInfo;
	private Hashtable<String, String> site2ClusterMap = new Hashtable<String, String>();
	private Hashtable<String,Integer> hitsPerSite;
	private GridContainer gridContainer = GridContainer.getInstance();

	public ClusterHead(String cid) {
		clusterId = cid;
	}
	
	/**
	 * Get the access records for each site from their queue
	 */
	private void getFileInfo() {
		fileInfo = new Hashtable<String, SiteRecord>(); //create a new one for every cycle
		for( Iterator iSite = gridContainer.iterateGridSites(); iSite.hasNext();) {
			GridSite site = (GridSite) iSite.next();
			if(site.getClusterId().equals(clusterId))
				continue;
			List<SiteRecord> queuedRecords = site.getAccessRecords();
			processQueuedInfo(queuedRecords);
		}
	}

	/**
	 * for each list of queued records from the site, update fileInfo table
	 * if "lfn:clusterId" key already exists, with number of hits
	 * @param records
	 */
	private void processQueuedInfo(List<SiteRecord> records) {    
		for(int i = 0; i < records.size(); i++) {    		
			SiteRecord recordInfo = records.get(i);
			String siteId = "" + recordInfo.getSiteId();
			String clusterId = recordInfo.getClusterId();
			String key = recordInfo.getLfn() + KEY_SEPARATOR + siteId;

			//key stored as "lfn:siteId"
			if(fileInfo.containsKey(key)) {
				SiteRecord oldRecord = fileInfo.get(key);
				updateHits(recordInfo, oldRecord);
			}
			if(!site2ClusterMap.containsKey(siteId)) {
				site2ClusterMap.put(siteId, clusterId);
			}
			fileInfo.put(key, recordInfo);  	
		}    
	}

	private void updateHits(SiteRecord newRecord, SiteRecord oldRecord) {
		if(newRecord.getSiteId() == oldRecord.getSiteId()) 
			newRecord.incrementHits(newRecord.getNumberOfHits());		
	}

	/**
	 * Requested from Policy Maker to send overall hits for each file
	 * @return Hastable<lfn,#hits>
	 */
	public Hashtable<String, Integer> getAggregateInfo() {
		getFileInfo();
		// create a new hashtable<lfn,#hits> and iterate through entire fileInfo hashtable
		Hashtable<String,Integer> aggregateInfo=new Hashtable<String,Integer>();
		Set<String> keys = fileInfo.keySet();
		for(String key: keys){
			String fileId=key.substring(0, key.indexOf(':'));
			if(!aggregateInfo.containsKey(fileId))
				aggregateInfo.put(fileId,fileInfo.get(key).getNumberOfHits());
			else
				aggregateInfo.put(fileId,aggregateInfo.get(fileId)+fileInfo.get(key).getNumberOfHits());
		}
		System.out.println("cluster id:" + clusterId +" fileInfo size at getAggregateInfo:" + fileInfo.size());
		Set<String> keys2 = aggregateInfo.keySet();
		for(String key: keys2){
			System.out.println("cluster id:" + clusterId + " lfn: " + key + " #hits:" + aggregateInfo.get(key));
		}
		return aggregateInfo;
	}

	/**
	 * computes the # hits per site for the popular file when called from
	 * Policy maker
	 * @param lfn of popularFile
	 * @return popularFileHitsPerSite
	 */
	public void getHitsPerSite(String popularFile) {
		hitsPerSite =new Hashtable<String,Integer>();
		//System.out.println("cluster id:" + clusterId + " fileInfo size at getHitsPerSite:" + fileInfo.size());
		Set<String> keys = fileInfo.keySet();
		for(String key: keys){
			String fileId=key.substring(0, key.indexOf(':'));
			if(fileId.equals(popularFile)){
				String siteId=key.substring(key.indexOf(':')+1);
				if(!hitsPerSite.containsKey(siteId))
					hitsPerSite.put(siteId,fileInfo.get(key).getNumberOfHits());
				else
					hitsPerSite.put(siteId,hitsPerSite.get(siteId)+fileInfo.get(key).getNumberOfHits());
			}
		}
		System.out.println("cluster id:" + clusterId + " hitsPerSite size at getHitsPerSite:" + hitsPerSite.size());
	}

	public Hashtable<String, Integer> getHistPerCluster(String popularFile) {
		getHitsPerSite(popularFile);
		Hashtable<String,Integer> popularFileHitsPerCluster=new Hashtable<String,Integer>();
		Set<String> keys = hitsPerSite.keySet();
		for(String siteId: keys){
			String cid = site2ClusterMap.get(siteId);
			if(!popularFileHitsPerCluster.containsKey(cid))
				popularFileHitsPerCluster.put(cid,hitsPerSite.get(siteId));
			else
				popularFileHitsPerCluster.put(cid,popularFileHitsPerCluster.get(cid)+
						hitsPerSite.get(siteId));
		}
		return popularFileHitsPerCluster;
	}
	
	public Hashtable<String, Integer> getHitsPerSiteTable(){
		return hitsPerSite;
	}
	
	public String getClusterId() {
		return clusterId;
	}
}
