package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    private int groupIndex;
    private int aggIndex;
    private Op op;
    private HashMap<Field,Integer> groupFields;
    private HashMap<Field,Integer> aggTotals;
    
    
    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        
    	// setting the fields
    	groupIndex = gbfield;
    	aggIndex = afield;
    	op = what;    
    	
    	// initializing hashmap
    	groupFields = new HashMap();
    	aggTotals = new HashMap();
    	
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
       
    	//create a variable for group field and the curretn agg val
    	Field currentGroupField = 
    			(groupIndex == Aggregator.NO_GROUPING) ? null : tup.getField(groupIndex);
    	int currentVal = 
    			((IntField) tup.getField(aggIndex)).getValue();
    	
    	if(!groupFields.containsKey(currentGroupField)){
    		
    		//store the field with associated value
    		groupFields.put(currentGroupField, currentVal);
    	}else{
    		
    		//getting the stored value
    		int newVal = -1;
    		int oldVal = groupFields.get(currentGroupField);
    		
    		
    		//depeding on the operation get the new value
    		switch(op){
    		
    		case MIN: 
    			if(currentVal < oldVal){
    				newVal = currentVal;
    			}
    			break;
    		case MAX:
    			if(currentVal > oldVal){
    				newVal = currentVal;
    			}
    			break;
    		case AVG:case SUM: 
    			newVal = currentVal + oldVal;
    			break;
			default:
				break;
    		}
    		
    		//storing the value we got
    		groupFields.put(currentGroupField, newVal);
    	}
    	
    }
    
    
    
    private TupleDesc createTupleDesc(){
	//creating the tuple descriptor for the iterator
    	TupleDesc tupleDesc;
    	Type[] typeArray;
    	String[] stringArray;
	
    	if(groupIndex == Aggregator.NO_GROUPING){
		
    		typeArray = new Type[]{Type.INT_TYPE};
    		stringArray = new String[]{"aggregateVal"};
		
    		tupleDesc = new TupleDesc(typeArray,stringArray);
		
    	}else{
    		typeArray = new Type[]{Type.INT_TYPE,Type.INT_TYPE};
    		stringArray = new String[]{"groupVal","aggregateVal"};
		
    		tupleDesc = new TupleDesc(typeArray,stringArray);
    	}
	
	return tupleDesc;
    }
    

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    private TupleDesc tupleDesc = createTupleDesc();
	ArrayList<Tuple> tupleList = new ArrayList<Tuple>();
	
	//creating tuple descriptor
	
    public DbIterator iterator() {
        
    	Tuple newTuple;
    	
    	for(Field currentKey: groupFields.keySet()){
    		
    		
    		// construct the tuples
        	newTuple = new Tuple(tupleDesc);
    		
    		// getting the new intField Value
    		IntField newVal = new IntField(groupFields.get(currentKey));
    		
    		
    		// if we are not grouping add single aggregate
    		if(groupIndex == Aggregator.NO_GROUPING){
    			
    			  newTuple.setField(0, newVal);
    			  
    		}else{
    			
    			//if we are grouping add both of the fields
    			newTuple.setField(0, currentKey);
    			newTuple.setField(1, newVal);
    			
    		}
        	//add the tuple in to the tuple iterable
    		tupleList.add(newTuple);
    	}
    	
    	
    	//iterator contructed using TupleIterator contructor
    	return new TupleIterator(tupleDesc, tupleList);
    	
    }

}
