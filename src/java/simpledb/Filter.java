package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    Predicate predicate;
    DbIterator child;
    
    public Filter(Predicate p, DbIterator child) {
    	
    	// setting predicate and child
        predicate = p;
        this.child = child;
    }

    public Predicate getPredicate() {
    	
    	// get the predicate
        return predicate;
    }

    public TupleDesc getTupleDesc() {
    	
        // get tuple descriptor
        return child.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
    	
    	// try to open
    	super.open();
        child.open();
        
    }

    public void close() {
    	
    	// try to close
    	super.close();
    	child.close();
    	
    	
    }

    public void rewind() throws DbException, TransactionAbortedException {
    	
    	//try to rewind
    	child.rewind();
        	
        
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        
    	while(child.hasNext()){
    		
        	Tuple returnTuple = child.next();
    		
    		if(predicate.filter(returnTuple)){
    			return returnTuple;
    		} 				
    	}
    	
    	return null;
    }

    @Override
    public DbIterator[] getChildren() {
    	
        return new DbIterator[] {child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        
    	child = children[0];
    	
    }

}
