package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {
    private int numFields;
    private TDItem[] fields;

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

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public TupleDescIterator iterator() {
        // changed return type to be specific to TupleDescIterator - avoid unchecked conversion warning
        // some code goes here
        return new TupleDescIterator(this);
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
        // some code goes here
        numFields = typeAr.length;
        fields = new TDItem[numFields];
                
        for (int i=0; i < numFields; i++){
            fields[i] = new TDItem(typeAr[i], fieldAr[i]);
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
        // some code goes here
        numFields = typeAr.length;
        fields = new TDItem[numFields];
                
        for (int i=0; i < numFields; i++){
            fields[i] = new TDItem(typeAr[i], null);
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return numFields;
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
        // some code goes here
        if (i > numFields) {
            throw new NoSuchElementException();
        }
        else {
            return fields[i].fieldName;
        }
        
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
        // some code goes here
        if (i > numFields) {
            throw new NoSuchElementException();
        }
        else {
            return fields[i].fieldType;
        }
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
        // some code goes here
        if (name == null) {
            throw new NoSuchElementException("null is not a valid field name");
        }
        int nullCount = 0;
        for (int i = 0; i < numFields; i++) {
            if (this.getFieldName(i) == null) {
                nullCount++;
                continue;
            }
            else if (this.getFieldName(i).equals(name)) {
                return i;
            } 
        }

        if (nullCount == numFields) {
            throw new NoSuchElementException("no fields are named, so you can't find it");
        }
        throw new NoSuchElementException(name + " is not a valid field name");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int byteSize = 0;
        for (int i=0; i < numFields; i++){
            byteSize += fields[i].fieldType.getLen();
        }
        return byteSize;
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
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        int newLength = td1.numFields() + td2.numFields();
        Type[] newTypeArr = new Type[newLength];
        String[] newStringArr = new String[newLength];
        for (int i=0; i<td1.numFields(); i++){
            newTypeArr[i] = td1.getFieldType(i);
            newStringArr[i] = td1.getFieldName(i);
        }
        
        for (int j=0; j<td2.numFields(); j++){
            newTypeArr[td1.numFields()+j] = td2.getFieldType(j);
            newStringArr[td1.numFields()+j] = td2.getFieldName(j);
        }
        TupleDesc mergedTuple = new TupleDesc(newTypeArr, newStringArr);
        return mergedTuple;
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
        // some code goes here
        TupleDesc copy;
        if (o == null) {
            return false;
        }
        try {
            copy = (TupleDesc) o;
        } catch (ClassCastException e) {
            return false;
        }
        if (this.getSize() != copy.getSize()) {
            return false;
        }
        for (int i=0; i < this.numFields; i++){
            if (this.getFieldType(i) != copy.getFieldType(i)){
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
        // some code goes here
        String output = "";

        for (int i=0; i<numFields; i++){
            output+=fields[i].toString() + ",";
        }
        return output;
    }

    protected class TupleDescIterator implements Iterator{

        private TupleDesc td;
        private int currentItem;

        public TupleDescIterator(TupleDesc td) {
            this.td = td;
            this.currentItem = 0;
        }

        public boolean hasNext(){
            return currentItem < td.numFields();
        }

        public TDItem next(){
            return td.fields[currentItem++];

        }
    }
}
