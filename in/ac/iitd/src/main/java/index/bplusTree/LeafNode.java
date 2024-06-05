package index.bplusTree;

import org.apache.calcite.util.Pair;

/*
    * A LeafNode contains keys and block ids.
    * Looks Like -
    * # entries | prev leafnode | next leafnode | ptr to next free offset | blockid_1 | len(key_1) | key_1 ...
    *
    * Note: Only write code where specified!
 */
public class LeafNode<T> extends BlockNode implements TreeNode<T>{

    Class<T> typeClass;
    public int parentBlockId;
    public LeafNode(Class<T> typeClass) {
        
        super();
        this.typeClass = typeClass;
        parentBlockId = 0;
        // set numEntries to 0
        byte[] numEntriesBytes = new byte[2];
        numEntriesBytes[0] = 0;
        numEntriesBytes[1] = 0;
        this.write_data(0, numEntriesBytes);

        // set ptr to next free offset to 8
        byte[] nextFreeOffsetBytes = new byte[2];
        nextFreeOffsetBytes[0] = 0;
        nextFreeOffsetBytes[1] = 8;
        this.write_data(6, nextFreeOffsetBytes);

        return;
    }

    // returns the keys in the node - will be evaluated
    @Override
    public T[] getKeys() {

        int numKeys = getNumKeys();
        T[] keys = (T[]) new Object[numKeys];
        int offset = 8;
        for(int i = 0; i < numKeys; ++i){
            offset += 2;
            byte[] keyLenBytes = this.get_data(offset, 2);
            offset += 2;
            int keyLength = (keyLenBytes[0] << 8) | (keyLenBytes[1] & 0xFF);
            byte[] keyBytes = this.get_data(offset, keyLength);
            keys[i] = this.convertBytesToT(keyBytes, typeClass);
            offset += keyLength;
        }
        return keys;
    }

    // returns the block ids in the node - will be evaluated
    public int[] getBlockIds() {

        int numKeys = getNumKeys();
        int[] block_ids = new int[numKeys];
        int offset = 8;
        for(int i = 0; i < numKeys; ++i){
            byte[] blockIdBytes = this.get_data(offset, 2);
            int blockId = (blockIdBytes[0] << 8) | (blockIdBytes[1] & 0xFF);
            block_ids[i] = blockId;
            offset += 2;
            byte[] keyLenBytes = this.get_data(offset, 2);
            offset += 2;
            int keyLength = (keyLenBytes[0] << 8) | (keyLenBytes[1] & 0xFF);
            offset += keyLength;
        }

        return block_ids;
    }

    // can be used as helper function - won't be evaluated
    @Override
    public void insert(T key, int block_id) {
        int numKeys = getNumKeys();
        byte[] keyBytes = convertTToBytes(key);
        int keyLength = keyBytes.length;
        byte[] keyLenBytes = halfintToByte(keyLength);
        byte[] blockIdBytes = halfintToByte(block_id);
        if(numKeys == 0){
            this.write_data(8, blockIdBytes);
            this.write_data(10, keyLenBytes);
            this.write_data(12, keyBytes);
            byte[] nextFreeOffsetBytes = halfintToByte(12 + keyLength);
            this.write_data(6, nextFreeOffsetBytes);
        }else{
            T[] keys = getKeys();
            byte[] nextFreeOffsetBytes = this.get_data(6,2);
            int nextFreeOffset = (nextFreeOffsetBytes[0] << 8) | (nextFreeOffsetBytes[1] & 0xFF);
            int offset = 8;
            if(compare(keys[0], key, typeClass) == true){
                offset += 2;
                offset += 2;
                int firstKeyLength  = convertTToBytes(keys[0]).length;
                offset += firstKeyLength;
                for(int i = 0; i < numKeys - 1; ++i){
                    if(compare(keys[i], key, typeClass) == true && compare(keys[i+1], key, typeClass) == false) break;
                    offset += 2;
                    byte[] iterativeKeyLenBytes = this.get_data(offset, 2);
                    offset += 2;
                    int iterativeKeyLength = (iterativeKeyLenBytes[0] << 8) | (iterativeKeyLenBytes[1] & 0xFF);
                    offset += iterativeKeyLength;
                }
            }
            byte[] copyableData = this.get_data(offset, nextFreeOffset - offset);
            this.write_data(offset, blockIdBytes);
            offset += 2;
            this.write_data(offset, keyLenBytes);
            offset += 2;
            this.write_data(offset, keyBytes);
            offset += keyLength;
            this.write_data(offset, copyableData);
            byte[] writableNextFreeOffsetBytes = halfintToByte(nextFreeOffset + keyLength + 4);
            this.write_data(6, writableNextFreeOffsetBytes);
        }
        byte[] newNumKeysBytes = halfintToByte(numKeys + 1);
        this.write_data(0, newNumKeysBytes);
    }

    public Pair<T,LeafNode<T>> splitLeafNode(T key, int block_id){
        int numKeys = getNumKeys();
        T[] keysBefore = getKeys();
        int[] blocksBefore = getBlockIds();
        T[] allKeys = (T[]) new Object[numKeys + 1];
        int[] allBlocks = new int[numKeys + 1];
        if(compare(keysBefore[0], key, typeClass) == false){
            allKeys[0] = key;
            allBlocks[0] = block_id;
            for(int i = 1; i <= numKeys; ++i){
                allKeys[i] = keysBefore[i-1];
                allBlocks[i] = blocksBefore[i-1];
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
                allBlocks[i] = blocksBefore[i];
            }
            allKeys[keyPos] = key;
            allBlocks[keyPos] = block_id;
            for(int i = keyPos + 1; i <= numKeys; ++i){
                allKeys[i] = keysBefore[i-1];
                allBlocks[i] = blocksBefore[i-1];
            }
        }
        int idnToPropagate = (numKeys + 1)/2;
        T keyToPropagate = allKeys[idnToPropagate];
        int leftEndOffset = 8;
        int numLeftKeys = idnToPropagate;
        // 0 to idnToPropagate - 1 in left
        // idnToPropagate to numKeys in Right, Also propagate idnToPropagate
        for(int i = 0; i < idnToPropagate; ++i){
            byte[] blockIdBytes = halfintToByte(allBlocks[i]);
            this.write_data(leftEndOffset, blockIdBytes);
            leftEndOffset += 2;
            byte[] keyBytes = convertTToBytes(allKeys[i]);
            int keyLength = keyBytes.length;
            byte[] keyLenBytes = halfintToByte(keyLength);
            this.write_data(leftEndOffset, keyLenBytes);
            leftEndOffset += 2;
            this.write_data(leftEndOffset, keyBytes);
            leftEndOffset += keyLength;
        }
        byte[] numLeftKeysBytes = halfintToByte(numLeftKeys);
        this.write_data(0, numLeftKeysBytes);
        byte[] leftEndOffsetBytes = halfintToByte(leftEndOffset);
        this.write_data(6, leftEndOffsetBytes);

        LeafNode<T> rightLeafNode = new LeafNode<>(typeClass);
        int rightNumKeys = numKeys + 1 - numLeftKeys;
        int rightEndOffset = 8;
        for(int i = idnToPropagate; i <= numKeys; ++i){
            byte[] blockIdBytes = halfintToByte(allBlocks[i]);
            rightLeafNode.write_data(rightEndOffset, blockIdBytes);
            rightEndOffset += 2;
            byte[] keyBytes = convertTToBytes(allKeys[i]);
            int keyLength = keyBytes.length;
            byte[] keyLenBytes = halfintToByte(keyLength);
            rightLeafNode.write_data(rightEndOffset, keyLenBytes);
            rightEndOffset += 2;
            rightLeafNode.write_data(rightEndOffset, keyBytes);
            rightEndOffset += keyLength;
        }
        byte[] numRightKeysBytes = halfintToByte(rightNumKeys);
        rightLeafNode.write_data(0, numRightKeysBytes);
        byte[] rightEndOffsetBytes = halfintToByte(rightEndOffset);
        rightLeafNode.write_data(6, rightEndOffsetBytes);
        Pair<T,LeafNode<T>> res = new Pair<> (keyToPropagate, rightLeafNode);
        return res;
    }
    // can be used as helper function - won't be evaluated
    @Override
    public int search(T key) {
        int numKeys = getNumKeys();
        T[] keys = getKeys();
        int[] block_ids = getBlockIds();
        for(int i = 0; i < numKeys; ++i){
            if(compare(keys[i], key, typeClass) == false && compare(key, keys[i], typeClass) == false) return block_ids[i];
        }
        return -1;
    }

}