package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    private int groupIndex;
    private int aggIndex;
    private DbIterator childIterator;
    private Aggregator.Op aggOp;
    
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
	
    	// setting contructor fields
    	groupIndex = gfield;
    	aggIndex = afield;
    	childIterator = child;
    	aggOp = aop;
    	
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	// some code goes here
    	return groupIndex;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
		
    	//if we aren't grouping
    	if(groupIndex == Aggregator.NO_GROUPING){
    		return null;
    	}
    	
    	//otherwise return the name
    	return childIterator.getTupleDesc().getFieldName(groupIndex);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
		
    	//returning the index
    	return aggIndex;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
    	//if we aren't grouping
    	//if(groupIndex == Aggregator.NO_GROUPING){
    	//	return null;
    	//}
    	
    	//otherwise return the name
    	return childIterator.getTupleDesc().getFieldName(aggIndex);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
    	
    	//return the operator
    	return aggOp;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
    	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
    	
    	
    	//super.open();
    	//childIterator.open();
    	
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
	// some code goes here
	return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
	// some code goes here
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
	// some code goes here
	return null;
    }

    public void close() {
	// some code goes here
    }

    @Override
    public DbIterator[] getChildren() {
	// some code goes here
	return null;
    }

    @Override
    public void setChildren(DbIterator[] children) {
	// some code goes here
    }
    
}
