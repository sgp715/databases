package simpledb;

import java.io.Serializable;
import java.util.*;
/* my imports */
import java.util.Vector;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /* tuple descriptor vector that holds TDItems */
    Vector<TDItem> tdVector;
    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        
        return tdVector.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
    	
    	/* creating a TDItem and vector to hold the descriptors*/
    	TDItem newTDItem;  
    	tdVector = new Vector<TDItem>(typeAr.length);
    	
    	/*iterate through all of the items and create an array of TDItems*/
        for(int i = 0; i < typeAr.length; i++){
        	newTDItem = new TDItem(typeAr[i], fieldAr[i]);
        	tdVector.add(newTDItem);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
    	/* TDItem and descriptor and empty description*/
    	TDItem newTDItem;  
    	tdVector = new Vector<TDItem>(typeAr.length);
    	String mt_String = null;
    	
    	/*iterate through all of the items and create an array of TDItems*/
        for(int i = 0; i < typeAr.length; i++){
        	newTDItem = new TDItem(typeAr[i], mt_String);
        	tdVector.add(newTDItem);
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return tdVector.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
    	if(i < 0 || i > numFields()){
    		throw new NoSuchElementException();
    	}
        TDItem indexTDItem = tdVector.get(i);
        return indexTDItem.fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
    	if(i < 0 || i > numFields())
    	{
    		throw new NoSuchElementException();
    	}
    	TDItem indexTDItem = tdVector.get(i);
        return indexTDItem.fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
    	/* throw exception for null search */
    	if(name==null){
    		throw new NoSuchElementException();
    	}
    	
    	String tempTDName = null;
       	/* iterate tds in td vector */
    	for(int i = 0; i < numFields(); i++){
    		tempTDName = getFieldName(i);
    		if(name.equals(tempTDName)){
    			return i;
    		}
    	}
    	
    	/* if we didn't find an element then throw an exception */
    	if(tempTDName==null){
    		throw new NoSuchElementException("all fields null");
    	}
    	
    	throw new NoSuchElementException("no fields match name");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
    	
    	int totalSize = 0;
    	int numberFields = numFields();
    	Type tempTDType;
    	
    	/* iterate over fields adding size for each */
    	for(int i=0; i < numberFields; i++){
    		tempTDType = getFieldType(i);
    		totalSize += tempTDType.getLen();
    	}
        return totalSize;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static void fillTypeArray(TupleDesc td, Type[] typeArray, String[] nameArray, int start){
    	
    	Type currentTDItemType;
    	String currentTDItemName;
    	int end = td.numFields();
    	int currentArrayIndex = start;
    	
    	for(int i = 0; i < end; i++){
    		/*creating the new TDItem */
    		currentTDItemType = td.getFieldType(i);
    		currentTDItemName = td.getFieldName(i);
    		
    		/*placing the names and td.numFields() + start types in the array */
    		typeArray[currentArrayIndex] = currentTDItemType;
    		nameArray[currentArrayIndex] = currentTDItemName;
    		
    		currentArrayIndex++;
    		
    	}
    	
    }
    
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {

    	Type[] typeArray = new Type[td1.numFields() + td2.numFields()];
    	String[] nameArray = new String[td1.numFields() + td2.numFields()];
    	int start1 = 0;
    	int start2 = td1.numFields();
    	
    	fillTypeArray(td1, typeArray, nameArray, start1);
    	fillTypeArray(td2, typeArray, nameArray, start2);
    	
    	TupleDesc td3 = new TupleDesc(typeArray,nameArray);
    	
    	return td3;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
    	
    	if(o==null){
    		return false;
    	}
    	if(!(o instanceof TupleDesc)){
    		return false;
    	}
    	
        /* if sizes are not equal return false */
        int sizeOfObject = ((TupleDesc) o).getSize();
        int sizeOfTupleDesc = getSize();
        if(sizeOfObject!=sizeOfTupleDesc){
        	return false;
        }
        
        for(int i = 0; i < numFields(); i++){
        	/* if fields are not equal return */
        	if(((TupleDesc) o).getFieldType(i)!=getFieldType(i)){
        			return false;
        		}
        	}
        	
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
    	String tupleString = "";
    	
        for(int i = 0; i < numFields(); i++){
        	
        	tupleString += getFieldType(i) + "[" + i + "](" + getFieldName(i) + "[" + i + "]),";
        }
        return tupleString;
    }
}
