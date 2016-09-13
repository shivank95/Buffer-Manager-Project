/* ... */

package bufmgr;

import java.io.*;
import java.util.*;

import diskmgr.*;
import global.*;
import chainexception.ChainException;

class FrameDesc implements GlobalConst {
	
	public int pagePID;
	
	public boolean dirty;
	
	public int pin_cnt;
	
	public int refFreq;
	
	public FrameDesc() {
		
		pagePID = INVALID_PAGEID;
		dirty = false;
		pin_cnt = 0;
		refFreq = 0;
	}
	
	//Returns pin count for frame page
	public int get_pinCnt() {
		return pin_cnt;
	}
	
	public int getPageNum() {
		return pagePID;
	}
	
	public int getRefFreq(){
		return refFreq;
	}
	
	//Pins the page of a frame and returns the incremented pin count
	public int pin() {
		refFreq += 1;
		pin_cnt += 1;
		return pin_cnt;
	}
	
	//unpins the page of a frame and returns the decremented pin count
	public int unpin() {
		
		if (pin_cnt > 0) {
			pin_cnt = pin_cnt - 1;
		}
		return pin_cnt;
	}
	
	
}

//Each instance of this class describes an entry into the hashtable
class Node {
	
	
	
	//public Node next;
	
	//pageNo is used in the hash function to find the directory pointing to the bucket
	public int pageNo;
	
	public int frameNo;
	
	/*
	public void setValue(Node v) {
		this.frameNo = v.frameNo;
		//this.next = v;
		this.pageNo = v.pageNo;
		this.pageNo.pid = v.pageNo.pid;
	}*/
	public Node(int pid, int frameNo) {
		this.pageNo = pid;
		this.frameNo = frameNo;
	}
}

//HashTable class to keep track of pages in the buffer pool. 
//It can insert, retrieve and remove pages from the hash table
class HashTbl implements GlobalConst {
	
	//Number of bucket arrays in the Hashtable
	private static final int HTSIZE = 23;
	
	//Linklist Array
	private LinkedList<Node> buckets2[];
	
	
	private int hash(PageId pageNo) {
	
		int a = 3;
		int b = 4;
		return ((a * pageNo.pid) + b) % HTSIZE;
	}
	
	//Constructor to initialize hashtable
	public HashTbl () {
		
		buckets2 = (LinkedList<Node>[]) new LinkedList[HTSIZE];
		
		for (int i = 0; i < buckets2.length; i++) {
			buckets2[i] = new LinkedList<Node>();
		}
		
	}
	
	//Inserts a Node into the hash table. Returns true if successful
	public boolean insert(PageId pageNo, int frameNo) {
		
		int index = hash(pageNo);
		
		Iterator<Node> it = buckets2[index].iterator();
		Node e = null;
		boolean found = false;
		
		while(it.hasNext() && !found) {
			e = it.next();
			if (e.pageNo == pageNo.pid){
				found = true;
				e.frameNo = frameNo;
				return false;
			}
		}
		
		//System.out.println("Inserting with Hash: " + index + " PageID: " + pageNo);
		
		buckets2[index].push(new Node(pageNo.pid, frameNo));
		
		return true;
		
	}
	
	//Retrieves the frameNo of a pageNo given as input from the hashtable.
	//If the pagNo is invalid, it returns INVALID_PAGE
	public int lookup(PageId pageNo) {

		if (pageNo.pid == INVALID_PAGEID) {
			//System.out.println("Lookup Return Invalid Page ID from lookup method");
			return INVALID_PAGEID;
		}
		
		int i = hash(pageNo);
		//Node n = buckets2[i].pop();
		
		//System.out.println("Look Up with Hash: " + i + " PageID: " + pageNo.pid);
		
		Iterator<Node> it = buckets2[i].iterator();
		//System.out.println("Temp List: " + ListToString(i));
		Node e = null;
		while (it.hasNext()) {
			e = it.next();
			//System.out.println("e.pageID: " + e.pageNo);
			if (e.pageNo == pageNo.pid) {
				return e.frameNo;
				
			}

		}
		
		return INVALID_PAGEID;
		
	}
	
	//Removes a page from the hashtable, ie, the buffer pool
	public boolean remove(PageId pageNo) {
		
		//PageId doesnt exist anyway
		if (pageNo.pid == INVALID_PAGEID) {
			return true;
		}
		
		int i = hash(pageNo);
		Iterator<Node> it = buckets2[i].iterator();
		Node e = null;
		int count = -1;
		while (it.hasNext() && count != -2) {
			count++;
			e = it.next();
			if (e.pageNo == pageNo.pid) {
				buckets2[i].remove(count);
				count = -2;
				return true;
			}

		}
		
		/*
		Node current = buckets2[i].pop();
		//Node prev = null;
		int cnt = 0;
		while (current != null) {
			
			if (current.pageNo == pageNo.pid) {
				
				buckets2[i].remove(cnt);
				
				//Removed, return true
				return true;
			}
			cnt++;
			current = buckets2[i].get(cnt);
		}*/
		
		//Not found
		System.err.println("ERROR: Page not found in HashTable!");
		return false;
		
	}
	
	//Printing the hashTable
	public void display() {
		
		Node n;
		
		System.out.println("****Hash Table***");
		
		for (int i = 0; i < HTSIZE; i++) {
			
			//n = buckets2[i].pop();
			
			/*while(n != null) {
				System.out.print("[" + n.pageNo.pid + ", " + n.frameNo + "]\t");
				n = n.next;
			}*/
			if (!buckets2[i].isEmpty())
				System.out.println(ListToString(i));
		}
	}
	
	String ListToString(int index) {
        String result = "";
        LinkedList<Node> current = buckets2[index];
        int cnt = 0;
        while(cnt < current.size()){
            Node n = current.get(cnt);
            result += "[" + n.pageNo + ", " + n.frameNo + "]" + " ";
            cnt++;
        }
        return "List: " + result;
}

	
	
}

public class BufMgr implements GlobalConst{

	
	//Instance of the hash table that keeps track of the buffer pool
	private HashTbl hashTable;
	
	//Count of buffer frames in the buffer pool
	private int numBuffers;
	
	//The buffer pool
	private Page[] bufPool;
	
	//Array from frame descriptors
	private FrameDesc[] frameTable;
	
	private Replacer replacer;
	
	private boolean isFull;
	
	private boolean isFullPinned;
	
	private int nextBufferID;
	
	private Queue<Integer> queue;
	
	//private DiskMgr diskMgr;
	
  /**
   * Create the BufMgr object.
   * Allocate pages (frames) for the buffer pool in main memory and
   * make the buffer manage aware that the replacement policy is
   * specified by replacerArg (i.e. HL, Clock, LRU, MRU etc.).
   *
   * @param numbufs number of buffers in the buffer pool.
   * @param replacerArg name of the buffer replacement policy.
   */

  public BufMgr(int numbufs, int lookAheadSize, String replacementPolicy) {
	  
	  numBuffers = 100;
	  //System.out.println("NUMBUFS STARTING: " + numBuffers);
	  frameTable = new FrameDesc[numBuffers];
	  bufPool = new Page[numBuffers];
	  isFull = false;
	  isFullPinned = false;
	  //diskMgr = new DiskMgr();
	  nextBufferID = 0;
	  //Initialize frameTable
	  for(int i = 0; i < numBuffers; i++) {
		  frameTable[i] = new FrameDesc();
	  }
	  
	  queue = new LinkedList<Integer>();
	  initializeQueue();
	  
	  hashTable = new HashTbl();
	  
	  replacer = new Replacer(this);
  }
  
  public void initializeQueue() {
		for (int i = 0; i < numBuffers; i++)
			queue.add(i);
	}
  
  public int getFirstEmptyFrame() throws BufferPoolExceededException {
		if (queue.size() == 0)
			throw new BufferPoolExceededException(null, "BUFFER_POOL_EXCEED");
		else
			return queue.poll();
	}
  
  public boolean isFullPinned() throws Exception {
	  replacer.updateCandidates();
	  if (replacer.candidates.size() > 0) {
		  return false;
	  }
	  else {
		  return true;
	  }
  }
  
  
  //Debugging
  private void displayHash() {
	  //hashTable.display();
  }


  /**
   * Pin a page.
   * First check if this page is already in the buffer pool.
   * If it is, increment the pin_count and return a pointer to this
   * page.  If the pin_count was 0 before the call, the page was a
   * replacement candidate, but is no longer a candidate.
   * If the page is not in the pool, choose a frame (from the
   * set of replacement candidates) to hold this page, read the
   * page (using the appropriate method from {\em diskmgr} package) and pin it.
   * Also, must write out the old page in chosen frame if it is dirty
   * before reading new page.  (You can assume that emptyPage==false for
   * this assignment.)
   *
   * @param Page_Id_in_a_DB page number in the minibase.
   * @param page the pointer poit to the page.
   * @param emptyPage true (empty page); false (non-empty page)
   */
  public void pinPage(PageId pageno, Page page, boolean emptyPage) throws Exception, ChainException{
	  
	  int frameNo = 0;
	  
	  //System.out.println("\nPageNo Initial: " + pageno);
	  //Check if page exists in buffer pool
	  frameNo = hashTable.lookup(pageno);
	  
	  //System.out.println("FrameNo: " + frameNo);
	  
	  if (frameNo >= 0) {
		  
		//Increment Pin Count
		  frameTable[frameNo].pin();
		//Return pointer to page
		  
		  if(frameTable[frameNo].get_pinCnt() == 0) {
			  queue.remove(frameNo);
		  }
		  
		  page.setpage(bufPool[frameNo].getpage());
		  
		  
		  
	  }
	  else {
		  
		  Page temp = new Page();
		  
		  //Check if buffer pool is full
		  if (isFull) {
			  
			  if (isFullPinned()) {
				  throw new BufferPoolExceededException(null, "BUFFER_POOL_EXCEED");
			  }
			  //System.out.println("Buffer Full");
			  int i = replacer.chooseVictim();
			  
			  //If page is dirty flush it
			  if (frameTable[i].dirty == true) {
				  flushPage(new PageId(frameTable[i].getPageNum()));
			  }
			  
			  //Remove old page from hashTable
			  hashTable.remove(new PageId(frameTable[i].getPageNum()));
			  
			  //Add to FrameTable
			  FrameDesc newP = new FrameDesc();
			  newP.dirty = false;
			  newP.pagePID = pageno.pid;
			  newP.pin_cnt = 1;
			  newP.refFreq = 1;
			  frameTable[i] = newP;
			  
			  
			  //Add to HashTable
			  hashTable.insert(new PageId(newP.pagePID), i);
			  

			  Minibase.DiskManager.read_page(pageno, temp);
			  
			//Add to bufPool
			  bufPool[i] = temp;
			  page.setpage(bufPool[i].getpage());
			  
		  }
		  else { //Buffer pool not full
			  
			  
			  
			  //System.out.println("Buffer NOT FULL! PageNo: " + pageno.pid);
			  //Read page from diskmgr package
			  
			  Page temp2 = new Page();
			  
			  Minibase.DiskManager.read_page(pageno, temp2);
			  
			  bufPool[nextBufferID] = temp2;
			  
			  page.setpage(bufPool[nextBufferID].getpage());
			  
			  int data = 0;
			  data = Convert.getIntValue (0, temp.getpage());
			  //System.out.println("Page Not Found\nPageId: " + pageno.pid + "Data: " + data + "FrameNo: " + nextBufferID);
			  
			  //Add to FrameTable
			  FrameDesc newP = new FrameDesc();
			  newP.dirty = false;
			  newP.pagePID = pageno.pid;
			  newP.pin_cnt = 1;
			  newP.refFreq = 1;
			  frameTable[nextBufferID] = newP;
			  
			//Add to HashTable
			  hashTable.insert(pageno, nextBufferID);
			  //hashTable.display();
			  
			  //Update BuffID which is basically the next FrameNo or makes the bufferPool full
			  updateBufID();
		  }
		  //Send pointer
		  
		  
	  }
	  
	  //Update replacer candidates
	  //replacer.updatePinnedCandidates(pageno);
	    
  };
  
  private void updateBufID() {
	  
	  //Try to find a frame which is null
	  
	  if (nextBufferID < bufPool.length) {
		  nextBufferID++;
	  }
	  if (nextBufferID >= bufPool.length) {
		  isFull = true;
	  }
  }



  /**
   * Unpin a page specified by a pageId.
   * This method should be called with dirty==true if the client has
   * modified the page.  If so, this call should set the dirty bit
   * for this frame.  Further, if pin_count&gt;0, this method should
   * decrement it. If pin_count=0 before this call, throw an exception
   * to report error.  (For testing purposes, we ask you to throw
   * an exception named PageUnpinnedException in case of error.)
   *
   * @param globalPageId_in_a_DB page number in the minibase.
   * @param dirty the dirty bit of the frame
   */

  public void unpinPage(PageId PageId_in_a_DB, boolean dirty) throws ChainException {
	  
	  
	  int frameNo;
	  
	  //System.out.println("\n\nUnpin calling Lookup");
	  frameNo = hashTable.lookup(PageId_in_a_DB);
	  
	  //System.out.println("Unpin FrameNo: " + frameNo);
	  
	  //Check frameNo
	  if (frameNo < 0) {
		  //System.out.println("Unpin FRAME NO LESS THAN 0");
		  throw new HashEntryNotFoundException(null,
					"BUFMGR:PAGE_UNPIN_FAILED");
	  }
	  
	  //Check PageId
	  if (frameTable[frameNo].pagePID == INVALID_PAGEID) {
		  //System.out.println("Unpin INVALID PAGE ID");
		  throw new ChainException();
	  }
	  
	  //Check pin count
	  if (frameTable[frameNo].get_pinCnt() > 0) {
		  frameTable[frameNo].unpin();
	  }
	  else {
		  //System.out.println("Unpin with PINCOUNT == 0!!!");
		  throw new PageUnpinnedException(null,
					"BUFMGR:PAGE_UNPIN_FAILED");
	  }
	  
	  if (dirty == true) {
		  frameTable[frameNo].dirty = true;
	  }
	  
	  //hashTable.display();
	  
	  
  };


  /**
   * Allocate new pages.
   * Call DB object to allocate a run of new pages and
   * find a frame in the buffer pool for the first page
   * and pin it. (This call allows a client of the Buffer Manager
   * to allocate pages on disk.) If buffer is full, i.e., you
   * can't find a frame for the first page, ask DB to deallocate
   * all these pages, and return null.
   *
   * @param firstpage the address of the first page.
   * @param howmany total number of allocated new pages.
   *
   * @return the first page id of the new pages.  null, if error.
   */

  public PageId newPage(Page firstpage, int howmany) throws Exception {
	  
	  //System.out.println("New Page calling pinPage");
	  
	  if (!isFullPinned()) {
		  PageId newPage = new PageId();
		  
		 Minibase.DiskManager.allocate_page(newPage, howmany);
		 
		 pinPage(newPage,firstpage,false);
		 return newPage;
	  }
	  else {
		  //Buffer FULL deallocate
		  return null;
	  }
  };


  /**
   * This method should be called to delete a page that is on disk.
   * This routine must call the method in diskmgr package to
   * deallocate the page.
   *
   * @param globalPageId the page number in the data base.
 * @throws IOException 
   */

  public void freePage(PageId globalPageId) throws ChainException, IOException {
	  
	  int frameNo;
	  frameNo = hashTable.lookup(globalPageId);
	  
	  //Check frameNo
	  if (frameNo < 0) {
		  Minibase.DiskManager.deallocate_page(new PageId(globalPageId.pid));
		  
	  }
	  else {
		  if (frameTable[frameNo].get_pinCnt() > 1){
			  throw new PagePinnedException(null, "FREEING_A_PAGE_FAILED");
			  
		  }
		  if (frameTable[frameNo].get_pinCnt() == 1){
			  unpinPage(new PageId(frameTable[frameNo].pagePID), frameTable[frameNo].dirty);	
		  }
		  if (frameTable[frameNo].dirty){
			  flushPage(globalPageId);
		  }
		  hashTable.remove(globalPageId);
		  bufPool[frameNo] = null;
		  frameTable[frameNo].pagePID = INVALID_PAGEID;
		  //frameTable[frameNo].pagePID = INVALID_PAGEID;
		  Minibase.DiskManager.deallocate_page(globalPageId);
	  }
	  
  };


  /**
   * Used to flush a particular page of the buffer pool to disk.
   * This method calls the write_page method of the diskmgr package.
   *
   * @param pageid the page number in the database.
 * @throws InvalidPageNumberException 
   */

  public void flushPage(PageId pageid) throws IOException, 
  FileIOException,
  InvalidPageNumberException {
	  int frameno = hashTable.lookup(pageid);
		if(frameno > -1){
			Page page = new Page(bufPool[frameno].getpage().clone());
				Minibase.DiskManager.write_page(pageid, page);
				frameTable[frameno].dirty = false;
				//int data = Convert.getIntValue (0, page.getpage());
			  	//System.out.println("written data from frame:"+ pageid.pid + "to disk : " + data);
		}
  };

  /** Flushes all pages of the buffer pool to disk
 * @throws IOException 
 * @throws InvalidPageNumberException 
 * @throws FileIOException 
   */

  public void flushAllPages() throws FileIOException, InvalidPageNumberException, IOException {
	  for(int i = 0; i < numBuffers; i++){
			if(frameTable[i].dirty == true){
				this.flushPage(new PageId(frameTable[i].pagePID));
			}
		}
  };


  /** Gets the total number of buffers.
   *
   * @return total number of buffer frames.
   */

  public int getNumBuffers() {
	  return frameTable.length;
  };


  /** Gets the total number of unpinned buffer frames.
   *
   * @return total number of unpinned buffer frames.
   */

  public int getNumUnpinned() {
	  
	  int cnt = 0;
	  
	  for (int i = 0; i < frameTable.length; i ++) {
		  if (frameTable[i] == null || frameTable[i].pagePID == INVALID_PAGEID || frameTable[i].get_pinCnt() == 0) {
			  cnt++;
		  }
	  }
	  
	  return cnt;
	  
  };
  
  //Getter for frame table
  public FrameDesc[] getFrameDesc() {
	  return frameTable;
  }
  
  //Getter for directory
  public HashTbl getDirecory() {
	  return hashTable;
  }
  

}
