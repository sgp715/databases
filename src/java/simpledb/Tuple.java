package simpledb;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Vector;

import simpledb.TupleDesc.TDItem;
//import TupleDesc.TDItem;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    
    /**
     * Create a new tuple with the specified schema (type).
     * 
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    
    /*vector which can hold types (a tuple) */
    private Vector<Field> tdVector;
    private TupleDesc tdTuple;
    
    public Tuple(TupleDesc td) {
        tdTuple = td;
        tdVector = new Vector<Field>(this.tdTuple.numFields());
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        return tdTuple;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    
    /* creating the record which can be modified */
    RecordId ridTuple = null;
    public RecordId getRecordId() {
        return ridTuple;
    }

    /**
     * Set the RecordId information for this tuple.
     * 
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        ridTuple = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     * 
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        tdVector.add(i, f);
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     * 
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        Field getFieldVal = tdVector.get(i);
        return getFieldVal;
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * 
     * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
     * 
     * where \t is any whitespace, except newline, and \n is a newline
     */
    public String toString() {
    	
    	/*  creating content string by iterating over fields */
    	String contentString = null;
        for(int i = 0; i < tdVector.size(); i++){
        	contentString += getField(i) + "\t";
        }
        contentString += "\n";
        
        return contentString;
    }
    
    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        return tdVector.iterator();
    }
    
    /**
     * reset the TupleDesc of thi tuple
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        tdTuple = td;
        tdVector = new Vector<Field>(this.tdTuple.numFields());
    }
}
