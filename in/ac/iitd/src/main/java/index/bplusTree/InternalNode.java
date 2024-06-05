package index.bplusTree;

import org.apache.calcite.util.Pair;

/*
    * Internal Node - num Keys | ptr to next free offset | P_1 | len(K_1) | K_1 | P_2 | len(K_2) | K_2 | ... | P_n
    * Only write code where specified

    * Remember that each Node is a block in the Index file, thus, P_i is the block_id of the child node
 */
public class InternalNode<T> extends BlockNode implements TreeNode<T> {

    // Class of the key
    Class<T> typeClass;
    public int parentBlockId;
    // Constructor - expects the key, left and right child ids
    public InternalNode(T key, int left_child_id, int right_child_id, Class<T> typeClass) {

        super();
        this.typeClass = typeClass;
        parentBlockId = 0;
        byte[] numKeysBytes = new byte[2];
        numKeysBytes[0] = 0;
        numKeysBytes[1] = 0;

        this.write_data(0, numKeysBytes);

        byte[] child_1 = new byte[2];
        child_1[0] = (byte) ((left_child_id >> 8) & 0xFF);
        child_1[1] = (byte) (left_child_id & 0xFF);

        this.write_data(4, child_1);

        byte[] nextFreeOffsetBytes = new byte[2];
        nextFreeOffsetBytes[0] = 0;
        nextFreeOffsetBytes[1] = 6;

        this.write_data(2, nextFreeOffsetBytes);

        // also calls the insert method
        this.insert(key, right_child_id);
        return;
    }

    // returns the keys in the node - will be evaluated
    @Override
    public T[] getKeys() {

        int numKeys = getNumKeys();
        T[] keys = (T[]) new Object[numKeys];
        int offset = 4;
        for(int i = 0; i < numKeys; ++i){
            offset += 2;
            byte[] keyLenBytes = this.get_data(offset,2);
            offset += 2;
            int keyLength = (keyLenBytes[0] << 8) | (keyLenBytes[1] & 0xFF);
            byte[] keyBytes = this.get_data(offset, keyLength);
            keys[i] = this.convertBytesToT(keyBytes, typeClass);
            offset += keyLength;
        }
        return keys;
    }

    // can be used as helper function - won't be evaluated
    @Override
    public void insert(T key, int right_block_id) {
        int numKeys = getNumKeys();
        byte[] keyBytes = convertTToBytes(key);
        int keyLength = keyBytes.length;
        byte[] keyLenBytes = halfintToByte(keyLength);
        byte[] rightBlockIdBytes = halfintToByte(right_block_id);
        if(numKeys == 0){
            this.write_data(6, keyLenBytes);
            this.write_data(8, keyBytes);
            this.write_data(8 + keyLength, rightBlockIdBytes);
            byte[] nextFreeOffsetBytes = halfintToByte(10 + keyLength);
            this.write_data(2, nextFreeOffsetBytes);
        }else{
            T[] keys = getKeys();
            byte[] nextFreeOffsetBytes = this.get_data(2, 2);
            int nextFreeOffset = (nextFreeOffsetBytes[0] << 8) | (nextFreeOffsetBytes[1] & 0xFF);
            int offset = 6;
            if(compare(key, keys[0], typeClass) == false){
                offset += 2;
                int firstKeyLength = convertTToBytes(keys[0]).length;
                offset += firstKeyLength;
                offset += 2;
                for(int i = 0; i < numKeys - 1; ++i){
                    if(compare(keys[i], key, typeClass) == true && compare(keys[i+1], key, typeClass) == false) break;
                    byte[] iterativeKeyLenBytes = this.get_data(offset, 2);
                    offset += 2;
                    int iterativeKeyLength = (iterativeKeyLenBytes[0] << 8) | (iterativeKeyLenBytes[1] & 0xFF);
                    offset += iterativeKeyLength;
                    offset += 2;
                }
            }
            byte[] copyableData = this.get_data(offset, nextFreeOffset - offset);
            this.write_data(offset, keyLenBytes);
            offset += 2;
            this.write_data(offset, keyBytes);
            offset += keyLength;
            this.write_data(offset, rightBlockIdBytes);
            offset += 2;
            this.write_data(offset, copyableData);
            byte[] writableNextFreeOffsetBytes = halfintToByte(nextFreeOffset + keyLength + 4);
            this.write_data(2, writableNextFreeOffsetBytes);
        }
        byte[] newNumKeysBytes = halfintToByte(numKeys + 1);
        this.write_data(0, newNumKeysBytes);
    }

    public Pair<T,InternalNode<T>> splitInternalNode(T key, int right_block_id){
        int numKeys = getNumKeys();
        T[] keysBefore = getKeys();
        int[] childrenBefore = getChildren();
        T[] allKeys = (T[])new Object[numKeys + 1];
        int[] allRightBlockIds = new int[numKeys + 1];
        if(compare(keysBefore[0], key, typeClass) == false){
            allKeys[0] = key;
            allRightBlockIds[0] = right_block_id;
            for(int i = 1; i <= numKeys; ++i){
                allKeys[i] = keysBefore[i-1];
                allRightBlockIds[i] = childrenBefore[i];
            }
        }else{
            int keyPos = numKeys;
            for(int i = 0; i < numKeys - 1; ++i){
                if(compare(keysBefore[i], key, typeClass) == true && compare(keysBefore[i+1], key, typeClass) == false){
                    keyPos = i+1;
                    break;
                }
            }
            for(int i = 0; i < keyPos; ++i){
                allKeys[i] = keysBefore[i];
                allRightBlockIds[i] = childrenBefore[i + 1];
            }
            allKeys[keyPos] = key;
            allRightBlockIds[keyPos] = right_block_id;
            for(int i = keyPos + 1; i <= numKeys; ++i){
                allKeys[i] = keysBefore[i - 1];
                allRightBlockIds[i] = childrenBefore[i];
            }
        }
        int idnToPropagate = (numKeys + 1)/2;
        // 0 to idnToPropagate-1 in Left
        // idnToPropagate + 1 to numkeys in Right
        T keyToPropagate = allKeys[idnToPropagate];
        int leftEndOffset = 6;
        int numLeftKeys = idnToPropagate;
        for(int i = 0; i < idnToPropagate; ++i){
            byte[] keyBytes = convertTToBytes(allKeys[i]);
            int keyLength = keyBytes.length;
            byte[] keyLenBytes = halfintToByte(keyLength);
            this.write_data(leftEndOffset, keyLenBytes);
            leftEndOffset += 2;
            this.write_data(leftEndOffset, keyBytes);
            leftEndOffset += keyLength;
            byte[] rightBlockIdBytes = halfintToByte(allRightBlockIds[i]);
            this.write_data(leftEndOffset, rightBlockIdBytes);
            leftEndOffset += 2;
        }
        byte[] numLeftKeysBytes = halfintToByte(numLeftKeys);
        this.write_data(0, numLeftKeysBytes);
        byte[] leftEndOffsetBytes = halfintToByte(leftEndOffset);
        this.write_data(2, leftEndOffsetBytes);

        InternalNode<T> rightInternalNode = new InternalNode(allKeys[idnToPropagate + 1], allRightBlockIds[idnToPropagate], allRightBlockIds[idnToPropagate + 1], typeClass);
        int numRightKeys = numKeys - idnToPropagate;
        int rightEndOffset = 6;
        for(int i = idnToPropagate + 1; i <= numKeys; ++i){
            byte[] keyBytes = convertTToBytes(allKeys[i]);
            int keyLength = keyBytes.length;
            byte[] keyLenBytes = halfintToByte(keyLength);
            rightInternalNode.write_data(rightEndOffset, keyLenBytes);
            rightEndOffset += 2;
            rightInternalNode.write_data(rightEndOffset, keyBytes);
            rightEndOffset += keyLength;
            byte[] rightBlockIdBytes = halfintToByte(allRightBlockIds[i]);
            rightInternalNode.write_data(rightEndOffset, rightBlockIdBytes);
            rightEndOffset += 2;
        }
        byte[] numRightKeysBytes = halfintToByte(numRightKeys);
        rightInternalNode.write_data(0, numRightKeysBytes);
        byte[] rightEndOffsetBytes = halfintToByte(rightEndOffset);
        rightInternalNode.write_data(2, rightEndOffsetBytes);
        Pair<T,InternalNode<T>> res = new Pair<> (keyToPropagate, rightInternalNode);
        return res;
    }

    // can be used as helper function - won't be evaluated
    @Override
    public int search(T key) {
        int numKeys = getNumKeys();
        int[] children = getChildren();
        T[] keys = getKeys();
        if(numKeys == 0) return -1;
        if(compare(key, keys[0], typeClass) == true) return children[0];
        for(int i = 0; i < numKeys - 1; ++i){
            if(compare(keys[i], key, typeClass) == true && compare(keys[i+1], key, typeClass) == false) return children[i+1];
        }
        return children[numKeys];
    }

    // should return the block_ids of the children - will be evaluated
    public int[] getChildren() {

        byte[] numKeysBytes = this.get_data(0, 2);
        int numKeys = (numKeysBytes[0] << 8) | (numKeysBytes[1] & 0xFF);

        int[] children = new int[numKeys + 1];
        int offset = 4;
        for(int i = 0; i < numKeys; ++i){
            byte[] childIdBytes = this.get_data(offset, 2);
            int childId = (childIdBytes[0] << 8) | (childIdBytes[1] & 0xFF);
            children[i] = childId;
            offset += 2;
            byte[] keyLenBytes = this.get_data(offset, 2);
            offset += 2;
            int keyLength = (keyLenBytes[0] << 8) | (keyLenBytes[1] & 0xFF);
            offset += keyLength;
        }
        byte[] childIdBytes = this.get_data(offset, 2);
        int childId = (childIdBytes[0] << 8) | (childIdBytes[1] & 0xFF);
        children[numKeys] = childId;
        return children;

    }

}