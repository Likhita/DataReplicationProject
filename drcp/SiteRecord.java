package org.edg.data.replication.drcp;

public class SiteRecord {

	private String lfn;
	private int siteId;
	private String clusterId;
	private int numberOfHits;
	private long timeStamp;

	public SiteRecord(String aLfn,int aSiteId, String cid, int aNumberOfHits,long aTimeStamp){
		lfn=aLfn;
		siteId=aSiteId;
		clusterId =cid;
		numberOfHits=aNumberOfHits;
		timeStamp=aTimeStamp;
	}

	public String getLfn(){
		return lfn;
	}

	public int getSiteId(){
		return siteId;
	}
	
	public String getClusterId(){
		return clusterId;
	}

	public int getNumberOfHits(){
		return numberOfHits;
	}

	public long getTimeStamp(){
		return timeStamp;
	}
	
	public void incrementHits(int count) {
		numberOfHits = numberOfHits + count;
	}
	
}
