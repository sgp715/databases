package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
	
	private File heapFileFile;
	private int heapID;
	private TupleDesc heapTD;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
    	
       heapFileFile = f;
       heapID = f.getAbsoluteFile().hashCode();
       heapTD = td;
    }

    /**
     * Returns the File backing this Hef.getAbsoluteFile().hashCode()apFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        
        return heapFileFile;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        
    	return heapID;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
    	return heapTD;
    }
    
    // see DbFile.java for javadocs
    public Page readPage(PageId pid){
    	int PageSize = BufferPool.getPageSize();
    	
    	try{
    		// opening the raf and getting offset 
    		RandomAccessFile accessedFile = new RandomAccessFile(heapFileFile,"r");
    		byte[] fileDataStream = new byte[PageSize];    
    		int offset = PageSize * pid.pageNumber();
    		// make sure we are not reading outside the file 
    		if(PageSize + offset > accessedFile.length()){
    			System.err.println("Attempted to read pag outside file");
    			System.exit(1);
    		}
        		
    		// reading page into byte array 
    	   	accessedFile.seek(offset);
    	   	accessedFile.readFully(fileDataStream);
    		accessedFile.close();
    		
    		// creating the new page */
    		Page redPage = new HeapPage((HeapPageId) pid, fileDataStream);
    		return redPage;
    		
    	}catch(Exception e){
    		throw new IllegalArgumentException("Attempted to read the file but failed");
    	}
    } 

    
    
    
    
    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
    	
    	int pageSize = BufferPool.getPageSize();
    	
    	//creating a random access file
        RandomAccessFile randomFile = new RandomAccessFile(heapFileFile,"rw");
        
        //creating a byte stream
        PageId pid = page.getId();
        
       //writing the actual data
       randomFile.seek(pageSize * pid.pageNumber());
       randomFile.write(page.getPageData(), 0, pageSize);
       randomFile.close(); 
        
    }

    
    
    
    
    
    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        
    	double tuple_size = 8 * BufferPool.getPageSize();
    	double file_size = (heapFileFile.length() * 8);
    	int tuplesPerPage = (int) Math.floor(file_size / tuple_size);
        return tuplesPerPage;
    }

    
    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        
    	//list to return
    	ArrayList<Page> insertList = new ArrayList<Page>();
    	
    	return null;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        
    	
        return null;
    }
    
    
    
    
    
    
public class HeapFileIterator implements DbFileIterator {
        	
       
        private  HeapFile heapFile;
    	private TransactionId tid;	
        
        public HeapFileIterator(TransactionId tid, HeapFile heapFile) {
            this.tid = tid;
            this.heapFile=heapFile;
             
        }
        
        private Iterator<Tuple> pageTupleIterator;
        private int pgNo;
        private Iterator<Tuple> getTupleIterator(int pgNumber) throws TransactionAbortedException, DbException{
            
            PageId pageId = new HeapPageId(heapFile.getId(), pgNumber);
            Page page = Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
            HeapPage heapPage = (HeapPage)page;
            Iterator<Tuple> tuplesIterator = heapPage.iterator();
            return  tuplesIterator;
            
        }
        
        @Override
        public void open() throws DbException, TransactionAbortedException{
        	//set the page to zero and get the first iterator
            pgNo = 0;
            pageTupleIterator = getTupleIterator(pgNo);
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
        	
            if(pageTupleIterator == null){
                return false;
            }
            
            //check if the iterator has a next page
            if(pageTupleIterator.hasNext()){
                return true;
            } else if (pgNo < heapFile.numPages()-1){
            	Iterator<Tuple> tIterator = getTupleIterator(pgNo + 1);
            	if(tIterator.hasNext()){
                    return true;
                } 
            } 
            
            return false;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException,
                NoSuchElementException {
        	
        	Tuple tupleReturn;
        	
        	//make sure the iterator is not null
        	if(pageTupleIterator != null){
        		
        		//if there is a next element return it otherwise get the next page
           		if(pageTupleIterator.hasNext()){
                	tupleReturn = pageTupleIterator.next();
                	return tupleReturn;
            	} else if(pgNo < heapFile.numPages()-1) {
                	pageTupleIterator = getTupleIterator(++pgNo);
                	if(pageTupleIterator.hasNext()){
                	tupleReturn = pageTupleIterator.next();
                	return tupleReturn;
                	}
            	}
        	}
        	
        	//if it was null there are no more elements
            throw new NoSuchElementException("Iterated through all tuples");
            
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();

        }

        @Override
        public void close() {
        	pageTupleIterator = null;

        }
} 
    


    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
    	
    	DbFileIterator dbIterator = new HeapFileIterator(tid, this);
        return dbIterator;
    }

}

