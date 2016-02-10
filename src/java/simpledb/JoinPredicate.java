package simpledb;

import java.io.Serializable;

/**
 * JoinPredicate compares fields of two tuples using a predicate. JoinPredicate
 * is most likely used by the Join operator.
 */
public class JoinPredicate implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor -- create a new predicate over two fields of two tuples.
     * 
     * @param field1
     *            The field index into the first tuple in the predicate
     * @param field2
     *            The field index into the second tuple in the predicate
     * @param op
     *            The operation to apply (as defined in Predicate.Op); either
     *            Predicate.Op.GREATER_THAN, Predicate.Op.LESS_THAN,
     *            Predicate.Op.EQUAL, Predicate.Op.GREATER_THAN_OR_EQ, or
     *            Predicate.Op.LESS_THAN_OR_EQ
     * @see Predicate
     */
    private int fField;
    private Predicate.Op predicateOperator;
    private int sField;
    
    public JoinPredicate(int field1, Predicate.Op op, int field2) {
      
    	// setting the two fields
    	fField = field1;
    	sField = field2;
    	
    	// setting predicate operator
    	predicateOperator = op;
    	
    }

    /**
     * Apply the predicate to the two specified tuples. The comparison can be
     * made through Field's compare method.
     * 
     * @return true if the tuples satisfy the predicate.
     */
    public boolean filter(Tuple t1, Tuple t2) {
        
    	// getting first field value
    	Field ff = t1.getField(fField);
    	Field sf = t2.getField(sField);
    	
    	return ff.compare(predicateOperator, sf);
    }
    
    public int getField1()
    {
        return fField;
    }
    
    public int getField2()
    {
        
        return sField;
    }
    
    public Predicate.Op getOperator()
    {
        return predicateOperator;
    }
}
