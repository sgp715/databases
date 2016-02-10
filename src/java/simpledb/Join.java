package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor. Accepts to children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    private JoinPredicate jp;
    private DbIterator fchild;
    private DbIterator schild;
    private static Tuple currentTuple;
    
    public Join(JoinPredicate p, DbIterator child1, DbIterator child2) {
        jp = p;
        fchild = child1;
        schild = child2;
        currentTuple = null;
    }

    public JoinPredicate getJoinPredicate() {
        return jp;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
    	
    	//get the name of the first child using tupledesc
        return fchild.getTupleDesc().getFieldName(jp.getField1());
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.  
     * */
    public String getJoinField2Name() {
    	
    	//get the name of the second child using tupledesc
    	return schild.getTupleDesc().getFieldName(jp.getField2());
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
    	

    	//calling the static merge method
        TupleDesc returnTD = TupleDesc.merge(fchild.getTupleDesc(), schild.getTupleDesc());
        
        return returnTD;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        super.open();
        fchild.open();
        schild.open();
    }

    public void close() {
    	super.close();
    	fchild.close();
    	schild.close();
        
    }

    public void rewind() throws DbException, TransactionAbortedException {
    	
    	 fchild.rewind();
         schild.rewind();
         currentTuple = null;
    }
    

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    
    boolean nested = true;
    Tuple fchildTuple;
    
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
		
    	//initialize the new tuple
	    Tuple returnTuple = new Tuple(this.getTupleDesc());
    	
    	//while there are tuples to check that satisfy
    	while(fchild.hasNext()){
    		
    		if(nested){
    			fchildTuple = fchild.next();
    			nested = false;
    		}
    		
    			while(schild.hasNext()){
    				
    				Tuple schildTuple = schild.next();

    				if(jp.filter(fchildTuple, schildTuple)){
    					
    					//index of the tuple field we are on
    					int ti = 0;
    			
    					//create the iterators so we can make the new tuple
    					Iterator<Field> fiterator = fchildTuple.fields();
    					Iterator<Field> siterator = schildTuple.fields();
    					
            	
    					//add in all the fields using the two iterators
    					while(fiterator.hasNext()){
    						returnTuple.setField(ti, fiterator.next());
    						ti++;
    					}
    					while(siterator.hasNext()){
    						returnTuple.setField(ti, siterator.next());
    						ti++;
    					}
    	    			return returnTuple;
    				}
    			}
    			
				//rewinding
    			nested = true;
	    		schild.rewind();    			
    	}
    		return null;
    }

    @Override
    public DbIterator[] getChildren() {
        
    	return new DbIterator[] {fchild, schild};
    }

    @Override
    public void setChildren(DbIterator[] children) {
    	fchild = children[0];
    	schild = children[1];
    }

}