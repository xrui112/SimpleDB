package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable  {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    private TDItem[] TDItems;
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
    public Iterator<TDItem> iterator() {
        // some code goes here

        return new TDItemIterator();
    }
    private class TDItemIterator implements Iterator<TDItem> {
        private int curindex=0;

        @Override
        public boolean hasNext() {
            return curindex<numFields();
        }

        @Override
        public TDItem next() {
            return TDItems[curindex++];
        }
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
        if(typeAr.length!=fieldAr.length) try {
            throw new DbException("Type array length must match field array length");
        } catch (DbException e) {
            throw new RuntimeException(e);
        }
        TDItems = new TDItem[typeAr.length];
        for (int i = 0; i < typeAr.length; i++) {
            TDItems[i]=new TDItem(typeAr[i],fieldAr[i]);
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
        TDItems = new TDItem[typeAr.length];
        for (int i = 0; i < typeAr.length; i++) {
            TDItems[i]=new TDItem(typeAr[i],null);
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return TDItems.length;
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
        if(i<0 || i>=numFields()){
            throw new NoSuchElementException("i不是合法的索引");
        }

        return TDItems[i].fieldName;
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
        if(i<0 || i>=numFields()){
            throw new NoSuchElementException("i不是合法的索引");
        }

        return TDItems[i].fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * No match if name is null.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        if(name==null){
            throw new NoSuchElementException("没有名为"+name+"的数据单元");
        }
        for(int i=0;i<numFields();i++){
            if(name.equals(TDItems[i].fieldName)){
                return i;
            }
        }

        throw new NoSuchElementException("没有名为"+name+"的数据单元");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     * @see Type#getSizeInBytes
     */
    public int getSizeInBytes() {
        // some code goes here
        int sum=0;
        for (int i = 0; i < numFields(); i++) {
            sum+=TDItems[i].fieldType.getSizeInBytes();
        }
        return sum;
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
        int len1=td1.numFields();
        int len2=td2.numFields();
        int len=len1+len2;
        Type[] types = new Type[len];
        String[] strings = new String[len];
        for (int i = 0; i < len1; i++) {
            types[i] = td1.getFieldType(i);
            strings[i] = td1.getFieldName(i);
        }
        for (int i = len1; i <len ; i++) {
            types[i] = td2.getFieldType(i-len1);
            strings[i] = td2.getFieldName(i-len1);
        }
        return new TupleDesc(types,strings);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i. It does not matter if the field names are equal.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        // some code goes here

        if(o==null){
            return false;
        }
        TupleDesc d1;
        if(o instanceof TupleDesc){
            d1=(TupleDesc) o;
        }else{
            return false;
        }
        if(this.numFields() != d1.numFields() ){
            return false;
        }else {
            for (int i = 0; i < d1.numFields(); i++) {
                if (!d1.getFieldType(i).equals(this.getFieldType(i))) {
                    return false;
                }
            }
            return true;
        }
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
            // 定义一个初始的哈希码值，可以是任何非零常数
            int result = 17;
            // 遍历每个字段，将字段的哈希码合并到结果中
            for (TDItem item : TDItems) {
                result = 31 * result + item.fieldType.hashCode();
                return result;
            }
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldName[0](fieldType[0]), ..., fieldName[M](fieldType[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        String t="";
        for (int i = 0; i < this.numFields(); i++) {
            t+=TDItems[i].fieldName+"("+TDItems[i].fieldType+")"+",";

        }
        return t;
    }
}
