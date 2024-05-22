package simpledb;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

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
    private Field[] fields;
    private RecordId recordId;
    private TupleDesc tupleDesc;

    public Tuple(TupleDesc td) {
        // some code goes here
        int num= td.numFields();
        fields=new Field[num];
        for (int i = 0; i < num; i++) {
            if(td.getFieldType(i)==Type.INT_TYPE){
                fields[i]=new IntField(0);

            }else if(td.getFieldType(i)==Type.STRING_TYPE){
                fields[i]=new StringField(null,Type.STRING_LEN);
            }else{
                throw new IllegalArgumentException("Unsupported field type");
            }
        }
        tupleDesc=td;
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk.
     *         Should return RecordId that was set with setRecordId(). May be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     * 
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
        this.recordId=rid;
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
        // some code goes here
        if (i < 0 || i >= fields.length) {
            throw new IllegalArgumentException("Invalid index");
        }
        fields[i]=f;

    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     * 
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        // some code goes here
        return fields[i];
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
        // some code goes here
        String s="";
        for (int i = 0; i < fields.length; i++) {
            s+=fields[i].toString()+" ";
        }
        return s;
    }
    
    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        // some code goes here
        return new fieldsIterator();
    }
    private class fieldsIterator implements Iterator<Field>{
        private int curindex=0;
        @Override
        public boolean hasNext() {
            return curindex<fields.length;
        }

        @Override
        public Field next() {
            return fields[curindex++];
        }
    }
    /**
     * Reset the TupleDesc of this tuple
     * (Only affecting the TupleDesc, does not need to worry about fields inside the Tuple)
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        tupleDesc=td;
        // some code goes here
    }
}
