package index.bplusTree;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import org.apache.commons.lang3.*; 
// TreeNode interface - will be implemented by InternalNode and LeafNode
public interface TreeNode <T> {

    public T[] getKeys();
    public void insert(T key, int block_id);

    public int search(T key);

    // DO NOT modify this - may be used for evaluation
    default public void print() {
        T[] keys = getKeys();
        for (T key : keys) {
            System.out.print(key + " ");
        }
        return;
    }
    
    // Might be useful for you - will not be evaluated
    default public T convertBytesToT(byte[] bytes, Class<T> typeClass){
        
        /* Write your code here */

        try {
            T convertedObj = (T) SerializationUtils.deserialize(bytes);
            return convertedObj;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
    
    default public byte[] convertTToBytes(T obj){
        try{
            ByteArrayOutputStream b = new ByteArrayOutputStream();
            ObjectOutputStream o = new ObjectOutputStream(b);
            o.writeObject(obj);
            o.flush();
            return b.toByteArray();
        }catch(Exception e){
            e.printStackTrace();
            return null;
        }
    }

    default public byte[] halfintToByte(int n){
        byte[] res = new byte[]{(byte)(n >> 8), (byte)n};
        return res;
    }

    default public boolean compare(T A, T B, Class<T> typeClass){ // returns true if A < B, else returns false
        if (typeClass.equals(String.class)) {
            String strA = (String) A;
            String strB = (String) B;
            return strA.compareTo(strB) < 0;
        } else if (typeClass.equals(Integer.class)) {
            Integer intA = (Integer) A;
            Integer intB = (Integer) B;
            return intA < intB;
        } else if (typeClass.equals(Boolean.class)) {
            Boolean boolA = (Boolean) A;
            Boolean boolB = (Boolean) B;
            return boolA.compareTo(boolB) < 0;
        } else if (typeClass.equals(Float.class)) {
            Float floatA = (Float) A;
            Float floatB = (Float) B;
            return floatA.compareTo(floatB) < 0;
        } else if (typeClass.equals(Double.class)) {
            Double doubleA = (Double) A;
            Double doubleB = (Double) B;
            return doubleA.compareTo(doubleB) < 0;
        } else {
            throw new IllegalArgumentException("Unsupported data type");
        }
    }
}