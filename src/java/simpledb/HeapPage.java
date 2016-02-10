package simpledb;

import java.util.*;
import java.io.*;

/**
 * Each instance of HeapPage stores data for one page of HeapFiles and 
 * implements the Page int try {
             RandomAccessFile f = new RandomAccessFile(this.heapFileFile,"r");
             int offset = BufferPool.getPageSize() * pid.pageNumber();
             byte[] data = new byte[BufferPool.getPageSize()];
             if (offset + BufferPool.getPageSize() > f.length()) {
                 System.err.println("Page offset exceeds max size, error!");
                 System.exit(1);
             }
             f.seek(offset);
             f.readFully(data);
             f.close();
             return new HeapPage((HeapPageId) pid, data);
         } catch (FileNotFoundException e) {
             System.err.println("FileNotFoundException: " + e.getMessage());
             throw new IllegalArgumentException();
         } catch (IOException e) {
             System.err.println("Caught IOException: " + e.getMessage());
             throw new IllegalArgumentException();
         }erface that is used by BufferPool.
 *
 * @see HeapFile
 * @see BufferPool
 *
 */
public class HeapPage implements Page {

    final HeapPageId pid;
    final TupleDesc td;
    final byte header[];
    final Tuple tuples[];
    final int numSlots;

    byte[] oldData;
    private final Byte oldDataLock=new Byte((byte)0);

    /**
     * Create a HeapPage from a set of bytes of data read from disk.
     * The format of a HeapPage is a set of header bytes indicating
     * the slots of the page that are in use, some number of tuple slots.
     *  Specifically, the number of tuples is equal to: <p>
     *          floor((BufferPool.getPageSize()*8) / (tuple size * 8 + 1))
     * <p> where tuple size is the size of tuples in this
     * database table, which can be determined via {@link Catalog#getTupleDesc}.
     * The number of 8-bit header words is equal to:
     * <p>
     *      ceiling(no. tuple slots / 8)
     * <p>
     * @see Database#getCatalog
     * @see Catalog#getTupleDesc
     * @see BufferPool#getPageSize()
     */
    public HeapPage(HeapPageId id, byte[] data) throws IOException {
        this.pid = id;
        this.td = Database.getCatalog().getTupleDesc(id.getTableId());
        this.numSlots = getNumTuples();
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        // allocate and read the header slots of this page
        header = new byte[getHeaderSize()];
        for (int i=0; i<header.length; i++)
            header[i] = dis.readByte();
        
        tuples = new Tuple[numSlots];
        try{
            // allocate and read the actual records of this page
            for (int i=0; i<tuples.length; i++)
                tuples[i] = readNextTuple(dis,i);
        }catch(NoSuchElementException e){
            e.printStackTrace();
        }
        dis.close();

        setBeforeImage();
    }

    /** Retrieve the number of tuples on this page.
        @return the number of tuples on this page
    */
    private int getNumTuples() {  
    	
    	/* return the number of tuples in a page */
        int tdSize = td.getSize();
        double dnumTuples = (BufferPool.getPageSize()*8) / ((tdSize * 8)+ 1);
        int numTuples = (int) Math.floor(dnumTuples);
        return numTuples;
    	

    }

    /**
     * Computes the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     * @return the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     */
    private int getHeaderSize() {   
    
    	//return the headersize for a page 
        double numTuples = this.getNumTuples();
        int headerSize = (int) Math.ceil(numTuples / 8);
        return headerSize;
                 
    }
    
    /** Return a view of this page before it was modified
        -- used by recovery */
    public HeapPage getBeforeImage(){
        try {
            byte[] oldDataRef = null;
            synchronized(oldDataLock)
            {
                oldDataRef = oldData;
            }
            return new HeapPage(pid,oldDataRef);
        } catch (IOException e) {
            e.printStackTrace();
            //should never happen -- we parsed it OK before!
            System.exit(1);
        }
        return null;
    }
    
    public void setBeforeImage() {
        synchronized(oldDataLock)
        {
        oldData = getPageData().clone();
        }
    }

    /**
     * @return the PageId associated with this page.
     */
    public HeapPageId getId() {
	    return pid;
    }

    /**
     * Suck up tuples from the source file.
     */
    private Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {
        // if associated bit is not set, read forward to the next tuple, and
        // return null.
        if (!isSlotUsed(slotId)) {
            for (int i=0; i<td.getSize(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty tuple");
                }
            }
            return null;
        }

        // read fields in the tuple
        Tuple t = new Tuple(td);
        RecordId rid = new RecordId(pid, slotId);
        t.setRecordId(rid);
        try {
            for (int j=0; j<td.numFields(); j++) {
                Field f = td.getFieldType(j).parse(dis);
                t.setField(j, f);
            }
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }

        return t;
    }

    /**
     * Generates a byte array representing the contents of this page.
     * Used to serialize this page to disk.
     * <p>
     * The invariant here is that it should be possible to pass the byte
     * array generated by getPageData to the HeapPage constructor and
     * have it produce an identical HeapPage object.
     *
     * @see #HeapPage
     * @return A byte array correspond to the bytes of this page.
     */
    public byte[] getPageData() {
        int len = BufferPool.getPageSize();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(baos);

        // create the header of the page
        for (int i=0; i<header.length; i++) {
            try {
                dos.writeByte(header[i]);
            } catch (IOException e) {
                // this really shouldn't happen
                e.printStackTrace();
            }
        }

        // create the tuples
        for (int i=0; i<tuples.length; i++) {

            // empty slot
            if (!isSlotUsed(i)) {
                for (int j=0; j<td.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
                continue;
            }

            // non-empty slot
            for (int j=0; j<td.numFields(); j++) {
                Field f = tuples[i].getField(j);
                try {
                    f.serialize(dos);
                
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // padding
        int zerolen = BufferPool.getPageSize() - (header.length + td.getSize() * tuples.length); //- numSlots * td.getSize();
        byte[] zeroes = new byte[zerolen];
        try {
            dos.write(zeroes, 0, zerolen);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    /**
     * Static method to generate a byte array corresponding to an empty
     * HeapPage.
     * Used to add new, empty pages to the file. Passing the results of
     * this method to the HeapPage constructor will create a HeapPage with
     * no valid tuples in it.
     *
     * @return The returned ByteArray.
     */
    public static byte[] createEmptyPageData() {
        int len = BufferPool.getPageSize();
        return new byte[len]; //all 0
    }

    /**
     * Delete the specified tuple from the page;  the tuple should be updated to reflect
     *   that it is no longer stored on any page.
     * @throws DbException if this tuple is not on this page, or tuple slot is
     *         already empty.
     * @param t The tuple to delete
     */
    public void deleteTuple(Tuple t) throws DbException {
        
    	//getting the record id
    	RecordId tupleRecordId = t.getRecordId();
    	PageId tuplePageId = tupleRecordId.getPageId();
    	
    	if(this.pid != tuplePageId){
            //throw exception if it is not on the page
    		throw new DbException("tuple not on page could not delete");
    	}
    	
    	//get the tuple number
    	int tupleNo = tupleRecordId.tupleno();
    	
    	if(isSlotUsed(tupleNo)){
        //or if the slot is already empty
    		throw new DbException("tuple is already empty");
    	}
    	
    	//otherwise modify page using
    	markSlotUsed(tupleNo,false);
    	tuples[tupleNo] = null;
    	
    }

    /**
     * Adds the specified tuple to the page;  the tuple should be updated to reflect
     *  that it is now stored on this page.
     * @throws DbException if the page is full (no empty slots) or tupledesc
     *         is mismatch.
     * @p
     * aram t The tuple to add.
     */
    public void insertTuple(Tuple t) throws DbException {
        
    	if(getNumEmptySlots() == 0){
            //if page is full	
        	throw new DbException("page is full");
        }
    	
    	//create an iterator
    	Iterator<Tuple> tupleIterator = this.iterator();
    	
    	//setting it to an insane value
    	int tupleIteratorNo = -1;
    	
    	//keep going until we find an empty one
    	while(tupleIterator.hasNext()){
    		
    		tupleIteratorNo = tupleIterator.next().getRecordId().tupleno();
    		
    		if(isSlotUsed(tupleIteratorNo)){
    		
    			if(t.getTupleDesc().equals(tuples[tupleIteratorNo].getTupleDesc())){
        		
    				break;
        		
        		//else{
    			// if there is a mismatch then no go
        		//throw new DbException("tuple mismatch");   
        		//}
        		
        		}else{
        			
        			// if there is a mismatch then no go
            		throw new DbException("tuple mismatch"); 
        		}
    		}
    		
    	}
    	
    	//if we found a vaule change the tuple
    	if(tupleIteratorNo != -1){
    		markSlotUsed(tupleIteratorNo,true);
    		tuples[tupleIteratorNo] = t;
    	}else{
    		//if page is full	
        	throw new DbException("page is full");
    	}
    	
    }

    /**
     * Marks this page as dirty/not dirty and record that transaction
     * that did the dirtying
     */
    
    TransactionId dirtyId = null;
    
    public void markDirty(boolean dirty, TransactionId tid) {
       
    	if(dirty){
    		//make it dirty
    		dirtyId = tid;
    		
    	}else{
    		//make it not dirty
    		dirtyId = null;
    	}
    	
    }

    /**
     * Returns the tid of the transaction that last dirtied this page, or null if the page is not dirty
     */
    public TransactionId isDirty() {
        
    	//the variable dirtyId is null if it is clean
        return dirtyId;      
    }

    /**
     * Returns the number of empty slots on this page.
     */
    public int getNumEmptySlots() {
    	
    	// variable to count number of empty slots 
    	int emptySlots = 0;
    	int numTuples = getNumTuples();
    	// iterate over bytes in header 
        for(int i = 0; i < numTuples; i++){
        	// if it is a 0 then empty 
        	if(!isSlotUsed(i))
        		emptySlots += 1;
        }
        
        return emptySlots;
    }

    /**
     * Returns true if associated slot on this page is filled.
     */
    public boolean isSlotUsed(int i) {
    	
    	if(i > getNumTuples())
    		return false;
    	
    	int tupleByteIndex = (int) Math.floor(i/8);
    	int tupleBitIndex = i % 8;
    	
    	byte tupleByte = header[tupleByteIndex];
    	int tupleBit = tupleByte & (1 << tupleBitIndex);
    	
    	if(tupleBit > 0){
    		return true;
    	}
    	
    	
    	return false;
    }

    /**
     * Abstraction to fill or clear a slot on this page.
     */
    private void markSlotUsed(int i, boolean value) {
    	
    	//if the index is to large get outta there
    	if(i > getNumTuples())
    		return;
    	
    	int tupleByteIndex = (int) Math.floor(i/8);
    	int tupleBitIndex = i % 8;
    	
    	
    	//getting the byte from the header
    	byte byteHeaderVal = header[tupleByteIndex];
    	
    	//setting the bit to 1 (aka marking as used)
    	if(value){
        	header[tupleByteIndex] = 
        			(byte) (byteHeaderVal | (byte) (1 << tupleBitIndex));
    	}else{
    		header[tupleByteIndex] = 
        			(byte) (byteHeaderVal & (byte) (~(1 << tupleBitIndex)));
    	}

    	
    }
    
    
    public class TupleIterator implements Iterator<Tuple>{
    	
    	HeapPage hp;
    	Tuple[] tupleArray;
    	public TupleIterator(HeapPage heapPage){
    		hp = heapPage;
    		tupleArray = heapPage.tuples; 		
    	}
    	
    	int numTuples = getNumTuples();
    	int tupleIndex = 0;
    	int tuplesFound = 0;
    	int fullTuples = getNumTuples() - getNumEmptySlots();
    	public boolean hasNext(){
    	
    		if(tuplesFound < fullTuples)
    			return true;
    		
    		return false;
    	}
    	
    	public Tuple next() throws NoSuchElementException{
    		
    		if(!this.hasNext())
    			throw new NoSuchElementException();
    		
  		    // check that the first slot is full 
    		boolean foundFull = hp.isSlotUsed(tupleIndex);
    		
    		if(foundFull){
    			tuplesFound++;
    			return tupleArray[tupleIndex++];
    		}else{
    			tupleIndex++;
    			return next();
    		}
    		
    	}
    	
    	public void remove() throws UnsupportedOperationException{
    		throw new UnsupportedOperationException();
    	}
    	
    }
    /**
     * @return an iterator over all tuples on this page (calling remove on this iterator throws an UnsupportedOperationException)
     * (note that this iterator shouldn't return tuples in empty slots!)
     */
    public Iterator<Tuple> iterator(){
        return new TupleIterator(this);
    }

}

