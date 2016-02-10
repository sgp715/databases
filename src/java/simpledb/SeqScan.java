package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

    private static final long serialVersionUID = 1L;

    
    private TransactionId tid;
    private int tableid;
    private String tableAlias;
    HeapFile heapFile;
    DbFileIterator dbFileIterator;
    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     * 
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        this.tid = tid;
        this.tableid = tableid;
        this.tableAlias = tableAlias;
        
        /* getting the iterator that will be used for scanning */
        dbFileIterator = Database.getCatalog().getDatabaseFile(tableid).iterator(tid);
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name // some code goes hereof the table in the catalog of the database
     * */
    public String getTableName() {
    	String tableName = Database.getCatalog().getTableName(tableid);
        return tableName;
    }
    
    /**
     * @return Return the alias of the table this operator scans. 
     * */
    public String getAlias()
    {
        return tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        this.tableid = tableid;
        this.tableAlias = tableAlias;
    }

    public SeqScan(TransactionId tid, int tableid) {
        this(tid, tableid, Database.getCatalog().getTableName(tableid));
    }

    public void open() throws DbException, TransactionAbortedException {
        dbFileIterator.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.
     * 
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     * @throws NoSuchElementException 
     */
    public TupleDesc getTupleDesc(){
    	
    		//
    		TupleDesc tempReturn = heapFile.getTupleDesc();
    		Type[] tdTypes = new Type[tempReturn.numFields()];
			String[] tdNames = new String[tempReturn.numFields()];
			
    		for(int i = 0; i < tempReturn.numFields(); i++){
    			tdTypes[i] = tempReturn.getFieldType(i);
    			tdNames[i] = tableAlias + tempReturn.getFieldName(i);
    		}

    		TupleDesc tdReturn = new TupleDesc(tdTypes, tdNames);
    		return tdReturn;
    		
    	 
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {

        return dbFileIterator.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
    	
    	if(dbFileIterator == null){
    		throw new NoSuchElementException();
    	}
    	
    	Tuple nextTuple = dbFileIterator.next();        
        return nextTuple;
    }

    public void close() {
        dbFileIterator.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
    	
        dbFileIterator.rewind();
    }
}
