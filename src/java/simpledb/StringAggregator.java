package simpledb;

import java.util.ArrayList;

import simpledb.Aggregator.Op;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */
    private int groupbyField;
    private Type groupbyType;
    private int aggField;
    private Op what;
    
  //things for iterator
  ArrayList<Field> groupField;
  ArrayList<Integer> count;
  TupleDesc currentTupleDesc;
    
    
    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        
    	// setting all the fields
    	groupbyField = gbfield;
    	groupbyType = gbfieldtype;
    	aggField = afield;
    	this.what = what;   
    	
    	//for merging and iterating
    	groupField = new ArrayList();
    	count = new ArrayList();
    	currentTupleDesc = null;
    	
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        
    	//getting specified fields
    	Field currentTupleGroup = tup.getField(groupbyField);
    	Field currentTupleAggregate = tup.getField(aggField);
    	
    	//the tuple that will be merged
    	Tuple newTuple;
    	
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        throw new UnsupportedOperationException("please implement me for lab2");
    }

}
