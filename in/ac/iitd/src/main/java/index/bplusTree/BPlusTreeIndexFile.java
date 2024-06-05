package index.bplusTree;

import storage.AbstractFile;

import java.util.Queue;

import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/*
    * Tree is a collection of BlockNodes
    * The first BlockNode is the metadata block - stores the order and the block_id of the root node

    * The total number of keys in all leaf nodes is the total number of records in the records file.
*/

public class BPlusTreeIndexFile<T> extends AbstractFile<BlockNode> {

    Class<T> typeClass;

    // Constructor - creates the metadata block and the root node
    public BPlusTreeIndexFile(int order, Class<T> typeClass) {
        
        super();
        this.typeClass = typeClass;
        BlockNode node = new BlockNode(); // the metadata block
        LeafNode<T> root = new LeafNode<>(typeClass);
        // 1st 2 bytes in metadata block is order
        byte[] orderBytes = new byte[2];
        orderBytes[0] = (byte) (order >> 8);
        orderBytes[1] = (byte) order;
        node.write_data(0, orderBytes);

        // next 2 bytes are for root_node_id, here 1
        byte[] rootNodeIdBytes = new byte[2];
        rootNodeIdBytes[0] = 0;
        rootNodeIdBytes[1] = 1;
        node.write_data(2, rootNodeIdBytes);

        // push these nodes to the blocks list
        blocks.add(node);
        blocks.add(root);
    }

    private boolean isFull(int id){
        // 0th block is metadata block
        assert(id > 0);
        return blocks.get(id).getNumKeys() == getOrder() - 1;
    }

    private int getRootId() {
        BlockNode node = blocks.get(0);
        byte[] rootBlockIdBytes = node.get_data(2, 2);
        return (rootBlockIdBytes[0] << 8) | (rootBlockIdBytes[1] & 0xFF);
    }

    public int getOrder() {
        BlockNode node = blocks.get(0);
        byte[] orderBytes = node.get_data(0, 2);
        return (orderBytes[0] << 8) | (orderBytes[1] & 0xFF);
    }

    private boolean isLeaf(BlockNode node){
        return node instanceof LeafNode;
    }

    private boolean isLeaf(int id){
        return isLeaf(blocks.get(id));
    }

    // will be evaluated
    public void insert(T key, int block_id) {
        int intoBlockId = search(key);
        insertHelper(key, block_id, intoBlockId);
    }

    public void insertHelper(T key, int block_id, int intoBlockId){
        int rootId = getRootId();
        if(isLeaf(intoBlockId)){
            LeafNode<T> leftLeafNode = (LeafNode) blocks.get(intoBlockId);
            if(isFull(intoBlockId)){
                Pair<T,LeafNode<T>> propagation = leftLeafNode.splitLeafNode(key, block_id);
                T keyToPropagate = propagation.getKey();
                LeafNode<T> rightLeafNode = propagation.getValue();
                rightLeafNode.parentBlockId = leftLeafNode.parentBlockId;
                blocks.add(rightLeafNode);
                int rightLeafBlockId = blocks.size() - 1;
                byte[] rightLeafBlockIdBytes = rightLeafNode.halfintToByte(rightLeafBlockId);
                leftLeafNode.write_data(4, rightLeafBlockIdBytes);
                byte[] leftLeafBlockIdBytes = leftLeafNode.halfintToByte(intoBlockId);
                rightLeafNode.write_data(2, leftLeafBlockIdBytes);
                int parentBlockId = leftLeafNode.parentBlockId;
                if(intoBlockId == rootId){
                    InternalNode<T> newRoot = new InternalNode(keyToPropagate,intoBlockId,rightLeafBlockId,typeClass);
                    blocks.add(newRoot);
                    int newRootId = blocks.size() - 1;
                    leftLeafNode.parentBlockId = newRootId;
                    rightLeafNode.parentBlockId = newRootId;
                    byte[] newRootIdBytes = leftLeafNode.halfintToByte(newRootId);
                    blocks.get(0).write_data(2, newRootIdBytes);
                }else{
                    insertHelper(keyToPropagate, rightLeafBlockId, parentBlockId);
                }
            }else{
                leftLeafNode.insert(key, block_id);
            }
        }else{
            InternalNode<T> leftIneternalNode = (InternalNode) blocks.get(intoBlockId);
            if(isFull(intoBlockId)){
                Pair<T,InternalNode<T>> propagation = leftIneternalNode.splitInternalNode(key, block_id);
                T keyToPropagate = propagation.getKey();
                InternalNode<T> righInternalNode = propagation.getValue();
                righInternalNode.parentBlockId = leftIneternalNode.parentBlockId;
                blocks.add(righInternalNode);
                int rightInternalNodeId = blocks.size() - 1;
                int[] leftChildren = leftIneternalNode.getChildren();
                for(int i : leftChildren){
                    BlockNode child = blocks.get(i);
                    if(isLeaf(child)){
                        LeafNode<T> leafChild = (LeafNode) child;
                        leafChild.parentBlockId = intoBlockId;
                    }else{
                        InternalNode<T> InternalChild = (InternalNode) child;
                        InternalChild.parentBlockId = intoBlockId;
                    }
                }
                int[] rightChildren = righInternalNode.getChildren();
                for(int i : rightChildren){
                    BlockNode child = blocks.get(i);
                    if(isLeaf(child)){
                        LeafNode<T> leafChild = (LeafNode) child;
                        leafChild.parentBlockId = rightInternalNodeId;
                    }else{
                        InternalNode<T> InternalChild = (InternalNode) child;
                        InternalChild.parentBlockId = rightInternalNodeId;
                    }
                }
                if(intoBlockId == rootId){
                    InternalNode<T> newRoot = new InternalNode(keyToPropagate,intoBlockId, rightInternalNodeId, typeClass);
                    blocks.add(newRoot);
                    int newRootId = blocks.size() - 1;
                    leftIneternalNode.parentBlockId = newRootId;
                    righInternalNode.parentBlockId = newRootId;
                    byte[] newRootIdBytes = leftIneternalNode.halfintToByte(newRootId);
                    blocks.get(0).write_data(2, newRootIdBytes);
                }else{
                    insertHelper(keyToPropagate, rightInternalNodeId, leftIneternalNode.parentBlockId);
                }
            }else{
                leftIneternalNode.insert(key, block_id);
            }
        }
    }

    // will be evaluated
    // returns the block_id of the leftmost leaf node containing the key
    public int search(T key) {
        int rootId = getRootId();
        return searchHelper(key, rootId);
    }

    public int searchHelper(T key, int rootBlockId){
        if(isLeaf(rootBlockId)){
            return rootBlockId;
        }else{
            InternalNode<T> internalRootNode = (InternalNode)blocks.get(rootBlockId);
            int deeperBlockId = internalRootNode.search(key);
            return searchHelper(key, deeperBlockId);
        }
    }

    // returns true if the key was found and deleted, false otherwise
    // (Optional for Assignment 3)
    public boolean delete(T key) {

        /* Write your code here */
        return false;
    }

    // DO NOT CHANGE THIS - will be used for evaluation
    public void print_bfs() {
        int root = getRootId();
        Queue<Integer> queue = new LinkedList<>();
        queue.add(root);
        while(!queue.isEmpty()) {
            int id = queue.remove();
            if(isLeaf(id)) {
                ((LeafNode<T>) blocks.get(id)).print();
            }
            else {
                ((InternalNode<T>) blocks.get(id)).print();
                int[] children = ((InternalNode<T>) blocks.get(id)).getChildren();
                for(int i = 0; i < children.length; i++) {
                    queue.add(children[i]);
                }
            }
        }
        return;
    }

    // DO NOT CHANGE THIS - will be used for evaluation
    public ArrayList<T> return_bfs() {
        int root = getRootId();
        Queue<Integer> queue = new LinkedList<>();
        ArrayList<T> bfs = new ArrayList<>();
        queue.add(root);
        while(!queue.isEmpty()) {
            int id = queue.remove();
            if(isLeaf(id)) {
                T[] keys = ((LeafNode<T>) blocks.get(id)).getKeys();
                for(int i = 0; i < keys.length; i++) {
                    bfs.add((T) keys[i]);
                }
            }
            else {
                T[] keys = ((InternalNode<T>) blocks.get(id)).getKeys();
                for(int i = 0; i < keys.length; i++) {
                    bfs.add((T) keys[i]);
                }
                int[] children = ((InternalNode<T>) blocks.get(id)).getChildren();
                for(int i = 0; i < children.length; i++) {
                    queue.add(children[i]);
                }
            }
        }
        return bfs;
    }

    public List<BlockNode> return_blocks(){
        return this.blocks;
    }

    public void print() {
        print_bfs();
        return;
    }

}