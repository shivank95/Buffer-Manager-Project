package bufmgr;


import global.PageId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.LinkedList;
import java.util.Queue;

import global.*;

//LRU replacer class
public class Replacer implements GlobalConst {
	

	private FrameDesc[] frameDesc;
	private byte bufPool;
	private HashTbl directory;
	public ArrayList<FrameDesc> candidates;
	
	private Queue<FrameDesc> candList;
	
	
	//Constructor
	public Replacer(BufMgr bufmgr) {
		
		frameDesc = bufmgr.getFrameDesc();
		directory = bufmgr.getDirecory();
		candidates = new ArrayList<>();
		
	}
	
	public void updateCandidates() throws Exception {
		//Update Candidates list
				candidates.clear();
				for (int i = 0; i < frameDesc.length; i++) {
					FrameDesc curVictim = frameDesc[i];
					
					
					
					if (curVictim.get_pinCnt() == 0 || curVictim.pagePID == INVALID_PAGEID) {
						
						candidates.add(curVictim);
					}
				}
	}
	
	public int chooseVictim() throws Exception {
		
		//int pos = -1;
		
		//Update Candidates list
		candidates.clear();
		for (int i = 0; i < frameDesc.length; i++) {
			FrameDesc curVictim = frameDesc[i];
			
			if (curVictim == null || curVictim.pagePID == INVALID_PAGEID) {
				return i;
			}
			
			if (curVictim.get_pinCnt() == 0 || curVictim.pagePID == INVALID_PAGEID) {
				
				candidates.add(curVictim);
			}
		}
		//System.out.println("Candidates Size: " + candidates.size());
		
		if (candidates.size() <= 0) {
			//System.out.println("\nSIZE == 0\n");
			return -1;
		}
		//Find lowest Ref count
		int lowest = candidates.get(0).getRefFreq();
		int frameNo = directory.lookup(new PageId(candidates.get(0).pagePID));
		//System.out.println("Initial Frame No.: " + frameNo);
		for(int i = 0; i < candidates.size(); i++) {
			
			int temp = candidates.get(i).getRefFreq();
			//System.out.println("Candidate " + i + ": PageID: " + candidates.get(i).pagePID + " FrameNo: "
			//+ directory.lookup(new PageId(candidates.get(i).pagePID)) + " Ref Count: " + temp);
			if ((temp < lowest && temp != -1) || (lowest == -1 && temp != -1)) {
				lowest = temp;
				frameNo = directory.lookup(new PageId(candidates.get(0).pagePID));
				if (frameNo == -1) {
					//System.out.println("Candidate " + i + ": " + candidates.get(i).pageNo.pid + " PINCNT: " + candidates.get(i).get_pinCnt());
				}
			}
		}
		
		//System.out.println("Final FRAME NO: " + frameNo);
		
		
		return frameNo;
	}
	
	/*
	//Finds a victim to be removed.
	public int findVictim() throws Exception {
		
		int pos = -1;
		
		for (int i = 0; i < candidates.size(); i++) {
			FrameDesc posVictim = candidates.get(i);
			
			if (posVictim.get_pinCnt() == 0) {
				candidates.remove(i);
				pos = directory.lookup(posVictim.pageNo);
				break;
			}
		}
		System.out.println("POS: " + pos);
		if (pos == INVALID_PAGEID || pos == -1) {
			throw new BufferPoolExceededException(null, null);
		}
		else {
			return pos;
		}
	}
	*/
	
	public void updatePinnedCandidates(PageId pgid) {
		
		FrameDesc cur = frameDesc[directory.lookup(pgid)];
		candList.remove(cur);
		candList.offer(cur);
		
	}
	
	public void removeFromCands(PageId pgid) {
		candidates.remove(frameDesc[directory.lookup(pgid)]);
	}
	
	public List<FrameDesc>  getLinkedList() {
		return candidates;
	}
	
}
