package optimizer.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.units.qual.m;

import index.bplusTree.BPlusTreeIndexFile;
import index.bplusTree.LeafNode;
import manager.StorageManager;
import storage.DB;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Stack;

import javax.swing.text.Style;

// Operator trigged when doing indexed scan
// Matches SFW queries with indexed columns in the WHERE clause
public class PIndexScan extends TableScan implements PRel {
    
        private final List<RexNode> projects;
        private final RelDataType rowType;
        private final RelOptTable table;
        private final RexNode filter;
    
        public PIndexScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, RexNode filter, List<RexNode> projects) {
            super(cluster, traitSet, table);
            this.table = table;
            this.rowType = deriveRowType();
            this.filter = filter;
            this.projects = projects;
        }
    
        @Override
        public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
            return new PIndexScan(getCluster(), traitSet, table, filter, projects);
        }
    
        @Override
        public RelOptTable getTable() {
            return table;
        }

        @Override
        public String toString() {
            return "PIndexScan";
        }

        public String getTableName() {
            return table.getQualifiedName().get(1);
        }

        @Override
        public List<Object[]> evaluate(StorageManager storage_manager) {
            String tableName = getTableName();
            System.out.println("Evaluating PIndexScan for table: " + tableName);

            /* Write your code here */
            List<String> columnNames = new ArrayList<>();
            List<SqlTypeName> columnTypes = new ArrayList<>();
            RelDataType relDataType = rowType;
            for (int i = 0; i < relDataType.getFieldCount(); i++) {
                columnNames.add(relDataType.getFieldList().get(i).getName());
                columnTypes.add(relDataType.getFieldList().get(i).getType().getSqlTypeName());
            }
            String filterCondition = filter.toString();
            System.out.println(filterCondition);
            Stack<String> stk = new Stack<>();
            String token = "";
            boolean operation = false;
            ArrayList<List<Object[]>> temp = new ArrayList<>();
            for(int i=0;i<filterCondition.length();i++){
                char c = filterCondition.charAt(i);
                if(c=='('){
                    if(token.equals("OR") || token.equals("AND")){
                        if(!token.equals(""))stk.push(token);
                        token = "";
                        continue;
                    }
                    operation = true;
                }
                else if(c==')'){
                    if(operation){
                        token+=c;
                        if(!token.equals(""))stk.push(token);
                        operation = false;
                        token = "";
                        continue;
                    }
                    else if(!operation){
                        String top = stk.pop();
                        while(!(top.equals("OR")|| top.equals("AND"))){
                            List<Object[]>obj_l = filter(storage_manager, tableName, columnTypes, columnNames, top);
                            temp.add(obj_l);
                            top = stk.pop();
                        }
                        if(top.equals("OR")){
                            List<Object[]> o1 = new ArrayList<>();
                            for(int arr=0;arr<temp.size();arr++){
                                List<Object[]>o2 = temp.get(arr);
                                o1 = union(o1, o2);
                            }
                            temp = new ArrayList<>();
                            temp.add(o1);
                        }
                        if(top.equals("AND")){
                            List<Object[]> o1 = temp.get(0);
                            for(int arr=0;arr<temp.size();arr++){
                                List<Object[]>o2 = temp.get(arr);
                                o1 = intersection(o1, o2);
                            }
                            temp = new ArrayList<>();
                            temp.add(o1);
                        }
                        continue;
                    }
                }
                else if(c==',' && filterCondition.charAt(i-1)==')'){
                    if(!token.equals(""))stk.push(token);
                    operation = false;
                    token = "";
                    continue;
                }
                else if(c==' ')continue;
                token+=c;
            }
            if(!token.equals(""))stk.push(token);
            if(!stk.empty()){
                List<Object[]>obj_l = filter(storage_manager, tableName, columnTypes, columnNames, stk.pop());
                temp.add(obj_l);
            }
            List<Object[]>final_out = new ArrayList<>();
            int[] indices = new int[projects.size()];
            for(int i=0;i<projects.size();i++){
                String s = projects.get(i).toString();
                String t = "";
                for(int j=0;j<s.length();j++){
                    char c = s.charAt(j);
                    if(c=='$')continue;
                    if(c==' ')continue;
                    else t+=c;
                }
                Integer ind = Integer.parseInt(t);
                indices[i] = ind;
            }
            for(int i=0;i<temp.get(0).size();i++){
                Object[] obj = new Object[indices.length];
                for(int j=0;j<indices.length;j++){
                    obj[j] = temp.get(0).get(i)[indices[j]];
                }
                final_out.add(obj);
            }
            if(temp.size()>0)return final_out;
            return null;
        }

        public List<Object[]> filter(StorageManager storageManager,String table_name,List<SqlTypeName> columnTypes,List<String> columnNames,String operator){
            String token = "";
            String operation = "";
            String col = "";
            String val = "";
            for(int i=0;i<operator.length();i++){
                char c = operator.charAt(i);
                if(c=='('){
                    operation = token;
                    token = "";
                    continue;
                }
                else if(c==','){
                    col = token;
                    token = "";
                    continue;
                }
                else if(c=='$' || c==')'){
                    continue;
                }
                token+=c;
            }
            val = token;
            int col_number = Integer.parseInt(col);
            String col_name = columnNames.get(col_number);
            SqlTypeName type = columnTypes.get(col_number);

            Object value = null;
            switch (type) {
                case INTEGER:
                    value = Integer.parseInt(val);
                    break;
                case BOOLEAN:
                    value = Boolean.parseBoolean(val);
                    break;
                case FLOAT:
                    value = Float.parseFloat(val);
                    break;
                case DOUBLE:
                    value = Double.parseDouble(val);
                    break;
                case VARCHAR:
                    value = val; // No need for casting for string type
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported column type: " + type);
            }
            RelDataTypeSystem typeSystem = RelDataTypeSystemImpl.DEFAULT; // Provide a valid implementation of RelDataTypeSystem
            RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(typeSystem); // Pass a valid CatalogReader if available
            RexBuilder rexBuilder = new RexBuilder(typeFactory);
            RexLiteral rexLiteral = null;
            switch (type) {
                case INTEGER:
                    rexLiteral = rexBuilder.makeExactLiteral(BigDecimal.valueOf((int) value));
                    break;
                case FLOAT:
                    rexLiteral = rexBuilder.makeExactLiteral(BigDecimal.valueOf((float) value));
                    break;
                case DOUBLE:
                    rexLiteral = rexBuilder.makeExactLiteral(BigDecimal.valueOf((double) value));
                    break;
                case BOOLEAN:
                    rexLiteral = rexBuilder.makeLiteral((boolean) value);
                    break;
                case VARCHAR:
                    rexLiteral = rexBuilder.makeLiteral((String) value);
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported column type: " + type);
            }
            String mod_name = table_name + "_" + col_name+"_index";
            int leaf_block_id = storageManager.search(table_name, col_name, rexLiteral);
            DB db = storageManager.getDb();
            int file_id = storageManager.get_file_id(mod_name);

            HashSet<Integer>record_id = new HashSet();
            List<Object[]>final_output = new ArrayList<>();
            if(operation.equals("=")){
                while(leaf_block_id!=0){
                    Object[] obj = leaf_node_breakdown(storageManager, db, file_id, leaf_block_id, type);
                    int next = (Integer)obj[1];
                    boolean end = false;
                    for(int i=2;i<obj.length;i+=2){
                        if(compare(obj[i], value, type)>0){
                            end = true;
                            break;
                        }
                        record_id.add((Integer)obj[i+1]);
                    }
                    if(end)break;
                    leaf_block_id = next;
                }
                for(Integer block_number: record_id){
                    List<Object[]> temp_output= storageManager.get_records_from_block(table_name, block_number);
                    for(Object[] obj: temp_output){
                        if(compare(obj[col_number], value, type)==0){
                            final_output.add(obj);
                        }
                    }
                }
            }
            else if(operation.equals(">")){
                while(leaf_block_id!=0){
                    Object[] obj = leaf_node_breakdown(storageManager, db, file_id, leaf_block_id, type);
                    int next = (Integer)obj[1];
                    for(int i=2;i<obj.length;i+=2){
                        if(compare(obj[i], value, type)<=0){
                            continue;
                        }
                        record_id.add((Integer)obj[i+1]);
                    }
                    leaf_block_id = next;
                }
                for(Integer block_number: record_id){
                    List<Object[]> temp_output= storageManager.get_records_from_block(table_name, block_number);
                    for(Object[] obj: temp_output){
                        if(compare(obj[col_number], value, type)>0){
                            final_output.add(obj);
                        }
                    }
                }
            }
            else if(operation.equals("<")){
                while(leaf_block_id!=0){
                    Object[] obj = leaf_node_breakdown(storageManager, db, file_id, leaf_block_id, type);
                    int prev = (Integer)obj[0];
                    for(int i=obj.length-2;i>0;i-=2){
                        if(compare(obj[i], value, type)>=0){
                            continue;
                        }
                        record_id.add((Integer)obj[i+1]);
                    }
                    leaf_block_id = prev;
                }
                for(Integer block_number: record_id){
                    List<Object[]> temp_output= storageManager.get_records_from_block(table_name, block_number);
                    for(Object[] obj: temp_output){
                        if(compare(obj[col_number], value, type)<0){
                            final_output.add(obj);
                        }
                    }
                }
            }
            else if(operation.equals(">=")){
                while(leaf_block_id!=0){
                    Object[] obj = leaf_node_breakdown(storageManager, db, file_id, leaf_block_id, type);
                    int next = (Integer)obj[1];
                    for(int i=2;i<obj.length;i+=2){
                        if(compare(obj[i], value, type)<0){
                            continue;
                        }
                        record_id.add((Integer)obj[i+1]);
                    }
                    leaf_block_id = next;
                }
                for(Integer block_number: record_id){
                    List<Object[]> temp_output= storageManager.get_records_from_block(table_name, block_number);
                    for(Object[] obj: temp_output){
                        if(compare(obj[col_number], value, type)>=0){
                            final_output.add(obj);
                        }
                    }
                }
            }
            else if(operation.equals("<=")){
                while(leaf_block_id!=0){
                    Object[] obj = leaf_node_breakdown(storageManager, db, file_id, leaf_block_id, type);
                    int prev = (Integer)obj[0];
                    for(int i=obj.length-2;i>0;i-=2){
                        if(compare(obj[i], value, type)>0){
                            continue;
                        }
                        record_id.add((Integer)obj[i+1]);
                    }
                    leaf_block_id = prev;
                }
                for(Integer block_number: record_id){
                    List<Object[]> temp_output= storageManager.get_records_from_block(table_name, block_number);
                    for(Object[] obj: temp_output){
                        if(compare(obj[col_number], value, type)<=0){
                            final_output.add(obj);
                        }
                    }
                }
            }
            return final_output;
        }

        public int compare(Object a, Object b, SqlTypeName type) {
            switch (type) {
                case INTEGER:
                    return Integer.compare((Integer) a, (Integer) b);
                case BOOLEAN:
                    boolean boolA = (Boolean) a;
                    boolean boolB = (Boolean) b;
                    return Boolean.compare(boolA, boolB);
                case FLOAT:
                    float floatA = (Float) a;
                    float floatB = (Float) b;
                    return Float.compare(floatA, floatB);
                case DOUBLE:
                    double doubleA = (Double) a;
                    double doubleB = (Double) b;
                    return Double.compare(doubleA, doubleB);
                case VARCHAR:
                    String stringA = (String) a;
                    String stringB = (String) b;
                    return stringA.compareTo(stringB);
                default:
                    throw new IllegalArgumentException("Unsupported column type: " + type);
            }
        }
        

        public Object[] leaf_node_breakdown(StorageManager storageManager,DB db,int file_id,int block_id, SqlTypeName type){
            byte[] no_of_keys_byte = db.get_data(file_id, block_id,0,2);
            int no_of_keys = (no_of_keys_byte[0]<<8) | (no_of_keys_byte[1] & 0xFF);
            byte[] prev_node_id_byte = db.get_data(file_id, block_id,2,2);
            int prev_node_id = (prev_node_id_byte[0]<<8) | (prev_node_id_byte[1] & 0xFF);
            byte[] next_node_id_byte = db.get_data(file_id, block_id,4,2);
            int next_node_id = (next_node_id_byte[0]<<8) | (next_node_id_byte[1] & 0xFF);

        /* Write your code here */
            Object[] objects = new  Object[2*no_of_keys + 2];
            objects[0] = prev_node_id;
            objects[1] = next_node_id;
            int offset = 8;
            for(int key=0;key<no_of_keys;key++){
                byte[] key_len_byte = db.get_data(file_id,block_id,offset+2,2); 
                int key_len = (key_len_byte[0]<<8) | (key_len_byte[1]& 0xFF);
                byte[] key_byte = db.get_data(file_id,block_id,offset+4,key_len);
                byte[] block_id_byte = db.get_data(file_id,block_id,offset,2);
                int map_id = (block_id_byte[0] << 8) | (block_id_byte[1] & 0xFF);
                Object obj = null;
                switch (type) {
                    case INTEGER:
                        LeafNode<Integer> inode = new LeafNode<>(Integer.class);
                        obj = inode.convertBytesToT(key_byte, Integer.class);
                        break;
                    case DOUBLE:
                        LeafNode<Double> dnode = new LeafNode<>(Double.class);
                        obj = dnode.convertBytesToT(key_byte,Double.class);
                        break;
                    case BOOLEAN:
                        LeafNode<Boolean>bNode = new LeafNode<>(Boolean.class);
                        obj = bNode.convertBytesToT(key_byte,Boolean.class);
                        break;
                    case FLOAT:
                        LeafNode<Float> fnode = new LeafNode<>(Float.class);
                        obj = fnode.convertBytesToT(key_byte, Float.class);
                        break;
                    case VARCHAR:
                        LeafNode<String>snode = new LeafNode<>(String.class);
                        obj = snode.convertBytesToT(key_byte,String.class);
                        break;
                    default:
                        break;
                }   
                offset+=4+key_len;
                objects[2 + 2*key] = obj;
                objects[2+2*key+1] = map_id; 
            }
            return objects;
        }

        public static List<Object[]> union(List<Object[]> list1, List<Object[]> list2) {
            HashMap<Object,Object[]>map = new HashMap<>();
            for(Object[] obj: list1){
                map.put(obj[0], obj);
            }
            for(Object[] obj: list2){
                map.put(obj[0], obj);
            }
            return new ArrayList<>(map.values());
        }
    

        public static List<Object[]> intersection(List<Object[]> list1, List<Object[]> list2) {
            HashMap<Object, Object[]> map = new HashMap<>();
            for (Object[] obj : list1) {
                map.put(obj[0], obj);
            }
        
            List<Object[]> intersectionList = new ArrayList<>();
            for (Object[] obj : list2) {
                if (map.containsKey(obj[0])) {
                    intersectionList.add(obj);
                }
            }
            return intersectionList;
        }        
}