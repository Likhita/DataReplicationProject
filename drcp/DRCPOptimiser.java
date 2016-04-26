package org.edg.data.replication.drcp;

import java.util.Iterator;
import java.util.List;

import org.edg.data.replication.optorsim.infrastructure.DataFile;
import org.edg.data.replication.optorsim.infrastructure.GridSite;
import org.edg.data.replication.optorsim.infrastructure.StorageElement;
import org.edg.data.replication.optorsim.optor.ReplicatingOptimiser;
import org.edg.data.replication.optorsim.reptorsim.ReplicaManager;

public class DRCPOptimiser extends ReplicatingOptimiser {

	/**
	 * toSite is the site which requested for popular file
	 * @param toSite
	 */
	public DRCPOptimiser(GridSite toSite) {
		super(toSite);
	}

/*	public void replicateFiles(double count, String popularFile) {

		ReplicaManager rm = ReplicaManager.getInstance();
		//String[] lfns = new String[1];
		//lfns[0] = popularFile;
		float[] fileFractions = new float[1];
		fileFractions[0] = (float)1.0;
		//DataFile[] source = super.getBestFile(lfns, fileFractions);
		DataFile[] source = rm.listReplicas(popularFile);
		if(source == null || source.length <= 0) {
			System.out.println("empty source file or pfns");
			return;
		} else
			System.out.println("size of source:" + source.length);

		StorageElement destinationSE = _site.getSE();

		for(int p = 0 ; p < source.length; p++) {
			System.out.println("source:" + source[p]);
		}

		if( destinationSE != null) {
			int i = -1;
			DataFile replicatedFile = null;
			DataFile pfn = null;
			do {
				i++;
				StorageElement se = source[i].se();
				// skip over any file stored on the local site
				if( se.getGridSite() == _site)
					continue;					
				// Check to see if there is a possibility of replication to close SE
				if(!destinationSE.isTherePotentialAvailableSpace(source[i]))
					continue;					
				// Loop trying to delete a file on closeSE to make space

				pfn = source[i];
				System.out.println("beofre replicating lfn/pfn: " + popularFile + "/" + pfn +
						" destination site: " + _site.exposeIndex());
				replicatedFile = rm.replicateFile( pfn, destinationSE);
				// Attempt to replicate file to close SE.	
				if(replicatedFile == null)
					System.out.println("no replication");

			} while( replicatedFile == null && i < source.length);	
			
			if(replicatedFile != null) {
				//now replicate to count-1 times
				for(int j=0; j < count-1; j++) {
					replicatedFile = rm.replicateFile( pfn, destinationSE);		
					System.out.println("multiple replicas lfn/pfn: " + popularFile + "/" + pfn +
							" destination site: " + _site.exposeIndex() + " count j :" + j);
					// If replication didn't work, try finding expendable files.
					List expendable = chooseFilesToDelete( pfn, destinationSE);
					// didn't find one, fall back to remoteIO
					if( expendable == null) {
						break;
					}					
					for (Iterator it = expendable.iterator(); it.hasNext() ;){
						rm.deleteFile( (DataFile) it.next());
					}
				}
			} else {
				System.out.println("replicated lfn/pfn: " + popularFile + "/" + pfn +
						" destination site: " + _site.exposeIndex());
			}
		}		
	}
*/
	protected List chooseFilesToDelete(DataFile file, StorageElement se) {
		return se.filesToDelete(file);
	}
}
