package manager;

import storage.DB;
import storage.File;
import storage.Block;
import Utils.CsvRowConverter;
import index.bplusTree.BPlusTreeIndexFile;
import index.bplusTree.LeafNode;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.BitSet;

import org.apache.calcite.linq4j.tree.SwitchCase;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.util.Sources;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.joestelmach.natty.generated.DateParser.date_return;

import java.util.Iterator;

public class StorageManager {

    private HashMap<String, Integer> file_to_fileid;
    private DB db;

    enum ColumnType {
        VARCHAR, INTEGER, BOOLEAN, FLOAT, DOUBLE
    };

    public StorageManager() {
        file_to_fileid = new HashMap<>();
        db = new DB();
    }

    // loads CSV files into DB362
    public void loadFile(String csvFile, List<RelDataType> typeList) {

        System.out.println("Loading file: " + csvFile);

        String table_name = csvFile;

        if(csvFile.endsWith(".csv")) {
            table_name = table_name.substring(0, table_name.length() - 4);
        }

        // check if file already exists
        assert(file_to_fileid.get(table_name) == null);

        File f = new File();
        try{
            csvFile = getFsPath() + "/" + csvFile;
            BufferedReader br = new BufferedReader(new FileReader(csvFile));
            String line = "";
            int lineNum = 0;

            while ((line = br.readLine()) != null) {

                // csv header line
                if(lineNum == 0){

                    String[] columnNames = CsvRowConverter.parseLine(line);
                    List<String> columnNamesList = new ArrayList<>();

                    for(String columnName : columnNames) {
                        // if columnName contains ":", then take part before ":"
                        String c = columnName;
                        if(c.contains(":")) {
                            c = c.split(":")[0];
                        }
                        columnNamesList.add(c);
                    }

                    Block schemaBlock = createSchemaBlock(columnNamesList, typeList);
                    f.add_block(schemaBlock);
                    lineNum++;
                    continue;
                }

                String[] parsedLine = CsvRowConverter.parseLine(line);
                Object[] row = new Object[parsedLine.length];

                for(int i = 0; i < parsedLine.length; i++) {
                    row[i] = CsvRowConverter.convert(typeList.get(i), parsedLine[i]);
                }

                // convert row to byte arrcolumn_nameay
                byte[] record = convertToByteArray(row, typeList);
                boolean added = f.add_record_to_last_block(record);
                if(!added) {
                    f.add_record_to_new_block(record);
                }
                lineNum++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }

        System.out.println("Done writing file\n");
        int counter = db.addFile(f);
        file_to_fileid.put(table_name, counter);
        return;
    }
    // converts a row to byte array to write to relational file
    private byte[] convertToByteArray(Object[] row, List<RelDataType> typeList) {

        List<Byte> fixed_length_Bytes = new ArrayList<>();
        List<Byte> variable_length_Bytes = new ArrayList<>();
        List<Integer> variable_length = new ArrayList<>();
        List<Boolean> fixed_length_nullBitmap = new ArrayList<>();
        List<Boolean> variable_length_nullBitmap = new ArrayList<>();

        for(int i = 0; i < row.length; i++) {

            if(typeList.get(i).getSqlTypeName().getName().equals("INTEGER")) {
                if(row[i] == null){
                    fixed_length_nullBitmap.add(true);
                    for(int j = 0; j < 4; j++) {
                        fixed_length_Bytes.add((byte) 0);
                    }
                } else {
                    fixed_length_nullBitmap.add(false);
                    int val = (int) row[i];
                    byte[] intBytes = new byte[4];
                    intBytes[0] = (byte) (val & 0xFF);
                    intBytes[1] = (byte) ((val >> 8) & 0xFF);
                    intBytes[2] = (byte) ((val >> 16) & 0xFF);
                    intBytes[3] = (byte) ((val >> 24) & 0xFF);
                    for(int j = 0; j < 4; j++) {
                        fixed_length_Bytes.add(intBytes[j]);
                    }
                }
            } else if(typeList.get(i).getSqlTypeName().getName().equals("VARCHAR")) {
                if(row[i] == null){
                    variable_length_nullBitmap.add(true);
                    for(int j = 0; j < 1; j++) {
                        variable_length_Bytes.add((byte) 0);
                    }
                } else {
                    variable_length_nullBitmap.add(false);
                    String val = (String) row[i];
                    byte[] strBytes = val.getBytes();
                    for(int j = 0; j < strBytes.length; j++) {
                        variable_length_Bytes.add(strBytes[j]);
                    }
                    variable_length.add(strBytes.length);
                }
            } else if (typeList.get(i).getSqlTypeName().getName().equals("BOOLEAN")) {         
                if(row[i] == null){
                    fixed_length_nullBitmap.add(true);
                    fixed_length_Bytes.add((byte) 0);
                } else {
                    fixed_length_nullBitmap.add(false);
                    boolean val = (boolean) row[i];
                    fixed_length_Bytes.add((byte) (val ? 1 : 0));
                }
            } else if (typeList.get(i).getSqlTypeName().getName().equals("FLOAT")) {
                
                if(row[i] == null){
                    fixed_length_nullBitmap.add(true);
                    for(int j = 0; j < 4; j++) {
                        fixed_length_Bytes.add((byte) 0);
                    }
                } else {
                    fixed_length_nullBitmap.add(false);
                    float val = (float) row[i];
                    byte[] floatBytes = new byte[4];
                    int intBits = Float.floatToIntBits(val);
                    floatBytes[0] = (byte) (intBits & 0xFF);
                    floatBytes[1] = (byte) ((intBits >> 8) & 0xFF);
                    floatBytes[2] = (byte) ((intBits >> 16) & 0xFF);
                    floatBytes[3] = (byte) ((intBits >> 24) & 0xFF);
                    for(int j = 0; j < 4; j++) {
                        fixed_length_Bytes.add(floatBytes[j]);
                    }
                }
            } else if (typeList.get(i).getSqlTypeName().getName().equals("DOUBLE")) {
                
                if(row[i] == null){
                    fixed_length_nullBitmap.add(true);
                    for(int j = 0; j < 8; j++) {
                        fixed_length_Bytes.add((byte) 0);
                    }
                } else {
                    fixed_length_nullBitmap.add(false);
                    double val = (double) row[i];
                    byte[] doubleBytes = new byte[8];
                    long longBits = Double.doubleToLongBits(val);
                    doubleBytes[0] = (byte) (longBits & 0xFF);
                    doubleBytes[1] = (byte) ((longBits >> 8) & 0xFF);
                    doubleBytes[2] = (byte) ((longBits >> 16) & 0xFF);
                    doubleBytes[3] = (byte) ((longBits >> 24) & 0xFF);
                    doubleBytes[4] = (byte) ((longBits >> 32) & 0xFF);
                    doubleBytes[5] = (byte) ((longBits >> 40) & 0xFF);
                    doubleBytes[6] = (byte) ((longBits >> 48) & 0xFF);
                    doubleBytes[7] = (byte) ((longBits >> 56) & 0xFF);
                    for(int j = 0; j < 8; j++) {
                        fixed_length_Bytes.add(doubleBytes[j]);
                    }
                }
            } else {
                System.out.println("Unsupported type");
                throw new RuntimeException("Unsupported type");
            }
        }

        short num_bytes_for_bitmap = (short) ((fixed_length_nullBitmap.size() + variable_length_nullBitmap.size() + 7) / 8); // should be in multiples of bytes

        //                       bytes for fixed length and variable length fields          offset & length of var fields
        byte[] result = new byte[fixed_length_Bytes.size() + variable_length_Bytes.size() + 4 * variable_length.size() + num_bytes_for_bitmap];
        int variable_length_offset = 4 * variable_length.size() + fixed_length_Bytes.size() + num_bytes_for_bitmap;

        int idx = 0;
        for(; idx < variable_length.size() ; idx ++){
            // first 2 bytes should be offset
            result[idx * 4] = (byte) (variable_length_offset & 0xFF);
            result[idx * 4 + 1] = (byte) ((variable_length_offset >> 8) & 0xFF);

            // next 2 bytes should be length
            result[idx * 4 + 2] = (byte) (variable_length.get(idx) & 0xFF);
            result[idx * 4 + 3] = (byte) ((variable_length.get(idx) >> 8) & 0xFF);

            variable_length_offset += variable_length.get(idx);
        }

        idx = idx * 4;

        // write fixed length fields
        for(int i = 0; i < fixed_length_Bytes.size(); i++, idx++) {
            result[idx] = fixed_length_Bytes.get(i);
        }

        // write null bitmap
        int bitmap_idx = 0;
        for(int i = 0; i < fixed_length_nullBitmap.size(); i++) {
            if(fixed_length_nullBitmap.get(i)) {
                result[idx] |= (1 << (7 - bitmap_idx));
            }
            bitmap_idx++;
            if(bitmap_idx == 8) {
                bitmap_idx = 0;
                idx++;
            }
        }
        for(int i = 0; i < variable_length_nullBitmap.size(); i++) {
            if(variable_length_nullBitmap.get(i)) {
                result[idx] |= (1 << (7 - bitmap_idx));
            }
            bitmap_idx++;
            if(bitmap_idx == 8) {
                bitmap_idx = 0;
                idx++;
            }
        }


        if(bitmap_idx != 0) {
            idx++;
        }

        // write variable length fields
        for(int i = 0; i < variable_length_Bytes.size(); i++, idx++) {
            result[idx] = variable_length_Bytes.get(i);
        }

        return result;
    }

    // helper function for loadFile
    private String getFsPath() throws IOException, ParseException {

        String modelPath = Sources.of(CsvRowConverter.class.getResource("/" + "model.json")).file().getAbsolutePath();
        JSONObject json = (JSONObject) new JSONParser().parse(new FileReader(modelPath));
        JSONArray schemas = (JSONArray) json.get("schemas");

        Iterator itr = schemas.iterator();

        while (itr.hasNext()) {
            JSONObject next = (JSONObject) itr.next();
            if (next.get("name").equals("FILM_DB")) {
                JSONObject operand = (JSONObject) next.get("operand");
                String directory = operand.get("directory").toString();
                return Sources.of(CsvRowConverter.class.getResource("/" + directory)).file().getAbsolutePath();
            }
        }
        return null;
    }

    // write schema block for a relational file
    private Block createSchemaBlock(List<String> columnNames, List<RelDataType> typeList) {

        Block schema = new Block();

        // write number of columns
        byte[] num_columns = new byte[2];
        num_columns[0] = (byte) (columnNames.size() & 0xFF);
        num_columns[1] = (byte) ((columnNames.size() >> 8) & 0xFF);

        schema.write_data(0, num_columns);

        int idx = 0, curr_offset = schema.get_block_capacity();
        for(int i = 0 ; i < columnNames.size() ; i ++){
            // if column type is fixed, then write it
            if(!typeList.get(i).getSqlTypeName().getName().equals("VARCHAR")) {
                
                // write offset
                curr_offset = curr_offset - (columnNames.get(i).length() + 2);
                byte[] offset = new byte[2];
                offset[0] = (byte) (curr_offset & 0xFF);
                offset[1] = (byte) ((curr_offset >> 8) & 0xFF);
                schema.write_data(2 + 2 * idx, offset);
                
                // convert column name to bytes
                byte[] column_name_type = new byte[columnNames.get(i).length() + 2];
                // first byte will tell datatype, 2nd byte will tell length of column name
                // Thus, assert that column name length is less than 256
                assert(columnNames.get(i).length() < 256);

                column_name_type[0] = (byte) (ColumnType.valueOf(typeList.get(i).getSqlTypeName().getName()).ordinal() & 0xFF);
                column_name_type[1] = (byte) (columnNames.get(i).length() & 0xFF);
                for(int j = 0; j < columnNames.get(i).length(); j++) {
                    column_name_type[2 + j] = (byte) columnNames.get(i).charAt(j);
                }

                schema.write_data(curr_offset, column_name_type);
                idx++;
            }
        }

        // write variable length fields
        for(int i = 0; i < columnNames.size(); i++) {
            if(typeList.get(i).getSqlTypeName().getName().equals("VARCHAR")) {
                
                // write offset
                curr_offset = curr_offset - (columnNames.get(i).length() + 2);
                byte[] offset = new byte[2];
                offset[0] = (byte) (curr_offset & 0xFF);
                offset[1] = (byte) ((curr_offset >> 8) & 0xFF); 
                // IMPORTANT: Take care of endianness
                schema.write_data(2 + 2 * idx, offset);
                
                // convert column name to bytes
                byte[] column_name_type = new byte[columnNames.get(i).length() + 2];
                // first byte will tell datatype, 2nd byte will tell length of column name
                // Thus, assert that column name length is less than 256
                assert(columnNames.get(i).length() < 256);

                column_name_type[0] = (byte) (ColumnType.valueOf(typeList.get(i).getSqlTypeName().getName()).ordinal() & 0xFF);
                column_name_type[1] = (byte) (columnNames.get(i).length() & 0xFF);
                for(int j = 0; j < columnNames.get(i).length(); j++) {
                    column_name_type[2 + j] = (byte) columnNames.get(i).charAt(j);
                }

                schema.write_data(curr_offset, column_name_type);
                idx++;
            }
        }

        return schema;
    }

    // should only read one block at a time
    public byte[] get_data_block(String table_name, int block_id){
        int file_id = file_to_fileid.get(table_name);
        return db.get_data(file_id, block_id);
    }

    public boolean check_file_exists(String table_name) {
        return file_to_fileid.get(table_name) != null;
    }

    public boolean check_index_exists(String table_name, String column_name) {
        String index_file_name = table_name + "_" + column_name + "_index";
        return file_to_fileid.get(index_file_name) != null;
    }

    public byte[] get_record_data(byte[] data,int offset,int length){
        if(offset + length > data.length){
            return null;
        }
        byte[] result = new byte[length];
        System.arraycopy(data, offset, result, 0, length);
        return result;
    }

    public static List<Boolean> byteArrayToBoolList(byte[] byteArray) {
        List<Boolean> boolList = new ArrayList<>();
        for(int b: byteArray){
            if(b==1){
                boolList.add(false);
            }
            else{
                boolList.add(true);
            }
        }
        return boolList;
    }

    public static int byteArrayToInt(byte[] bytes) {
        if (bytes.length != 4) {
            throw new IllegalArgumentException("Byte array length must be 4 to convert to an integer");
        }
        int value = 0;
        for (int i = 0; i < 4; i++) {
            value |= (bytes[3-i] & 0xFF) << (8 * (3 - i));
        }
        return value;
    }

    public static double byteArrayToDouble(byte[] bytes) {
        if (bytes.length != 8) {
            throw new IllegalArgumentException("Byte array length must be 8 to convert to a double");
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        return buffer.getDouble();
    }

    public static float byteArrayToFloat(byte[] bytes) {
        if (bytes.length != 4) {
            throw new IllegalArgumentException("Byte array length must be 4 to convert to a float");
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        return buffer.getFloat();
    }

    public int get_file_id(String filename){
        return file_to_fileid.get(filename);
    }


    // the order of returned columns should be same as the order in schema
    // i.e., first all fixed length columns, then all variable length columns
    public List<Object[]> get_records_from_block(String table_name, int block_id){
        /* Write your code here */
        // return null if file does not exist, or block_id is invalid
        // return list of records otherwise
        if(!check_file_exists(table_name))return null;
        int file_id = file_to_fileid.get(table_name);
        byte[] no_of_cols_byte = db.get_data(file_id, 0, 0, 2);
        if(no_of_cols_byte==null)return null;
        int no_of_cols = (no_of_cols_byte[1]<<8) | (no_of_cols_byte[0] & 0xFF);
        ArrayList<ColumnType> col_type_list = new ArrayList<>();
        ArrayList<String> col_name_list = new ArrayList<>(); 
        for(int i=0;i<no_of_cols;i++){
            byte[] col_offset_byte = db.get_data(file_id, 0, 2 + i*2, 2);
            int col_offset = (col_offset_byte[1]<<8) | (col_offset_byte[0] & 0xFF);
            byte[] col_type_byte = db.get_data(file_id, 0,col_offset,1);
            ColumnType col_type = convertByteToType(col_type_byte);
            byte[] len_col_name_byte = db.get_data(file_id, 0,col_offset + 1,1);
            int len_col_name = (len_col_name_byte[0] & 0xFF);
            byte[] column_name_byte = db.get_data(file_id, 0, col_offset + 2, len_col_name);
            String col_name = new String(column_name_byte,StandardCharsets.UTF_8);
            col_type_list.add(col_type);
            col_name_list.add(col_name);
        }
        int no_of_fixed_length = 0;
        int no_of_variable_length = 0;
        for(int i=0;i<col_type_list.size();i++){
           if(col_type_list.get(i).equals(ColumnType.VARCHAR)){
            no_of_variable_length++;
           }
           else no_of_fixed_length++;
        }
        List<Object[]>return_list = new ArrayList<>();
        byte[] no_of_records_byte = db.get_data(file_id, block_id, 0, 2);
        int no_of_records = (no_of_records_byte[0]<<8) | (no_of_records_byte[1] & 0xFF);
        int end_offset_record = 4096;
        for(int record_id = 0;record_id<no_of_records;record_id++){
            byte[] r_offset_b = db.get_data(file_id, block_id,2 + record_id*2,2);
            int r_offset = (r_offset_b[0]<<8) | (r_offset_b[1] & 0xFF);
            int r_len = end_offset_record - r_offset;
            end_offset_record = r_offset;
            byte[] record = db.get_data(file_id, block_id,r_offset,r_len);

            int fixed_var_start_offset = no_of_variable_length * 4;
            Object[] fixed_len_obj = new Object[no_of_fixed_length];
            int var_no = 0;
            for(;var_no<no_of_fixed_length;var_no++){
                ColumnType type = col_type_list.get(var_no);
                byte[] obj_byte;
                Object obj = null;
                // VARCHAR, INTEGER, BOOLEAN, FLOAT, DOUBLE
                switch (type) {
                    case INTEGER:
                        obj_byte = get_record_data(record, fixed_var_start_offset, 4);
                        obj = byteArrayToInt(obj_byte);
                        fixed_var_start_offset+=4;
                        break;
                    case BOOLEAN:
                        obj_byte = get_record_data(record, fixed_var_start_offset, 1);
                        obj = (obj_byte[0]!=0);
                        fixed_var_start_offset+=1;
                        break;
                    case FLOAT:
                        obj_byte = get_record_data(record, fixed_var_start_offset, 4);
                        obj = byteArrayToFloat(obj_byte);
                        fixed_var_start_offset+=4;
                        break;
                    case DOUBLE:
                        obj_byte = get_record_data(record, fixed_var_start_offset, 8);
                        obj = byteArrayToDouble(obj_byte);
                        fixed_var_start_offset+=8;
                        break;
                    default:
                        obj_byte = null;
                        break;
                }
                fixed_len_obj[var_no] = obj;
            }
            int bitmap_start_offset = fixed_var_start_offset;
            byte[] fixed_len_bitmap_byte = get_record_data(record, bitmap_start_offset, no_of_fixed_length);
            List<Boolean> fixed_len_bitmap = byteArrayToBoolList(fixed_len_bitmap_byte);
            for(int i=0;i<fixed_len_bitmap.size();i++){
                if(!fixed_len_bitmap.get(i)){
                    fixed_len_obj[i] = null;
                }
            }

            byte[] variable_len_bitmap_byte = get_record_data(record, bitmap_start_offset, no_of_variable_length);
            List<Boolean> variable_len_bitmap = byteArrayToBoolList(variable_len_bitmap_byte);
            Object[] variable_len_obj = new Object[no_of_variable_length];
            for(int i=0;i<no_of_variable_length;i++){
                byte[] obj_offset_byte = get_record_data(record, 4*i, 2);
                int obj_offset = (obj_offset_byte[1]<<8) | (obj_offset_byte[0] & 0xFF);
                byte[] obj_len_byte = get_record_data(record, 4*i + 2, 2);
                int obj_len = (obj_len_byte[1]<<8) | (obj_len_byte[0] & 0xFF);
                byte[] obj_byte = get_record_data(record, obj_offset, obj_len);
                Object obj = new String(obj_byte,StandardCharsets.UTF_8);
                variable_len_obj[i] = obj;
                if(!variable_len_bitmap.get(i))variable_len_obj[i] =null;
            }
            Object[] object_list = new Object[no_of_fixed_length + no_of_variable_length];
            int i=0;
            for(Object o: fixed_len_obj){
                object_list[i] = o;
                i++;
            }
            for(Object o: variable_len_obj){
                object_list[i] = o;
                i++;
            }
            return_list.add(object_list);
        }
        return return_list;
    }

    public Object byte_to_String(byte[] Byte){
        Object obj = new String(Byte,StandardCharsets.UTF_8);
        return obj;
    }

    public ColumnType convertByteToType(byte[] Byte){
        byte byteValue = Byte[0];
        int ordinalValue = byteValue & 0xFF;
        ColumnType columnType = ColumnType.values()[ordinalValue];
        return columnType;
    }

 

    public boolean create_index(String table_name, String column_name, int order) {
        /* Write your code here */
        String file_name = table_name + "_" + column_name + "_index";
        if(!check_file_exists(table_name))return false;
        int file_id = file_to_fileid.get(table_name);
        int total_number_of_records = db.get_num_records(file_id);
        byte[] no_of_cols_byte = db.get_data(file_id, 0, 0, 2);
        int no_of_cols = (no_of_cols_byte[0] & 0xFF) | (no_of_cols_byte[1]<<8);
        int column_number = -1;
        ColumnType col_type = null;
        for(int i=0;i<no_of_cols;i++){
            byte[] col_offset_byte = db.get_data(file_id, 0, 2 + i*2, 2);
            int col_offset = (col_offset_byte[0] & 0xFF) | (col_offset_byte[1] << 8);
            byte[] col_type_byte = db.get_data(file_id, 0,col_offset,1);
            col_type = convertByteToType(col_type_byte);
            byte[] len_col_name_byte = db.get_data(file_id, 0,col_offset + 1,1);
            int len_col_name = (len_col_name_byte[0] & 0xFF);
            byte[] column_name_byte = db.get_data(file_id, 0, col_offset + 2, len_col_name);
            String col_name = new String(column_name_byte,StandardCharsets.UTF_8);
            if(col_name.equals(column_name)){
                column_number = i;
                break;
            }
        }
        if(column_number<0)return false;
                // VARCHAR, INTEGER, BOOLEAN, FLOAT, DOUBLE
        BPlusTreeIndexFile<Integer>itree = null;
        BPlusTreeIndexFile<Double>dtree = null;
        BPlusTreeIndexFile<Boolean>btree = null;
        BPlusTreeIndexFile<Float>ftree = null;
        BPlusTreeIndexFile<String>vtree = null;
        switch (col_type) {
            case INTEGER:
                itree = new BPlusTreeIndexFile<>(order, Integer.class);
                break;
            case DOUBLE:
                dtree = new BPlusTreeIndexFile<>(order, Double.class);
                break;
            case BOOLEAN:
                btree = new BPlusTreeIndexFile<>(order, Boolean.class);
                break;
            case FLOAT:
                ftree = new BPlusTreeIndexFile<>(order, Float.class);
                break;
            case VARCHAR:
                vtree = new BPlusTreeIndexFile<>(order, String.class);
                break;
            default:
                break;
        }
        int no_of_retrieved_records = 0;
        int block_id = 1;
        while(!(no_of_retrieved_records==total_number_of_records)){
            byte[] data = db.get_data(file_id, block_id);
            if(data==null){
                continue;
            }
            List<Object[]>records = get_records_from_block(table_name, block_id);
            for(int obj_no = 0;obj_no<records.size();obj_no++){
                Object[] obj = records.get(obj_no);
                if(obj[column_number]==null)continue;
                switch (col_type) {
                    case INTEGER:
                        itree.insert((Integer)obj[column_number],block_id);
                        break;
                    case DOUBLE:
                        dtree.insert((Double)obj[column_number], block_id);
                        break;
                    case BOOLEAN:
                        btree.insert((Boolean)obj[column_number], block_id);
                        break;
                    case FLOAT:
                        ftree.insert((Float)obj[column_number], block_id);
                        break;
                    case VARCHAR:
                        vtree.insert((String)obj[column_number], block_id);
                        break;
                    default:
                        break;
                }   
            }
            no_of_retrieved_records+=records.size();
            block_id++;
        }
        int tree_id = -1;
        switch (col_type) {
            case INTEGER:
                tree_id = db.addFile(itree);
                break;
            case DOUBLE:
                tree_id = db.addFile(dtree);
                break;
            case BOOLEAN:
                tree_id = db.addFile(btree);
                break;
            case FLOAT:
                tree_id = db.addFile(ftree);
                break;
            case VARCHAR:
                tree_id = db.addFile(vtree);
                break;
            default:
                break;
        }   
        if(tree_id<0)return false;
        file_to_fileid.put(file_name, tree_id);
        return true;
    }

    // returns the block_id of the leaf node where the key is present
    public int search(String table_name, String column_name, RexLiteral value) {
        /* Write your code here */
        if(!check_file_exists(table_name))return -1;
        int file_id = file_to_fileid.get(table_name);
        int total_number_of_records = db.get_num_records(file_id);
        byte[] no_of_cols_byte = db.get_data(file_id, 0, 0, 2);
        int no_of_cols = (no_of_cols_byte[0] & 0xFF) | (no_of_cols_byte[1]<<8);
        int column_number = -1;
        ColumnType col_type = null;
        for(int i=0;i<no_of_cols;i++){
            byte[] col_offset_byte = db.get_data(file_id, 0, 2 + i*2, 2);
            int col_offset = (col_offset_byte[0] & 0xFF) | (col_offset_byte[1] << 8);
            byte[] col_type_byte = db.get_data(file_id, 0,col_offset,1);
            col_type = convertByteToType(col_type_byte);
            byte[] len_col_name_byte = db.get_data(file_id, 0,col_offset + 1,1);
            int len_col_name = (len_col_name_byte[0] & 0xFF);
            byte[] column_name_byte = db.get_data(file_id, 0, col_offset + 2, len_col_name);
            String col_name = new String(column_name_byte,StandardCharsets.UTF_8);
            if(col_name.equals(column_name)){
                column_number = i;
                break;
            }
        }
        String filename = table_name + "_" + column_name + "_index";
        file_id = file_to_fileid.get(filename);
        Object o_value = castRexLiteral(value, col_type);
        int leaf_node_block_id = db.search_index(file_id, o_value);
        return leaf_node_block_id;
    }

    public Object castRexLiteral(RexLiteral rexLiteral, ColumnType colType) {
        switch (colType) {
            case INTEGER:
                return rexLiteral.getValueAs(Integer.class);
            case FLOAT:
                return rexLiteral.getValueAs(Float.class);
            case DOUBLE:
                return rexLiteral.getValueAs(Double.class);
            case BOOLEAN:
                return rexLiteral.getValueAs(Boolean.class);
            case VARCHAR:
                return rexLiteral.getValueAs(String.class);
            default:
                throw new IllegalArgumentException("Unsupported column type: " + colType);
        }
    }


    public boolean delete(String table_name, String column_name, RexLiteral value) {
        /* Write your code here */
        // Hint: You need to delete from both - the file and the index
        return false;
    }

    // will be used for evaluation - DO NOT modify
    public DB getDb() {
        return db;
    }

    public <T> ArrayList<T> return_bfs_index(String table_name, String column_name) {
        if(check_index_exists(table_name, column_name)) {
            int file_id = file_to_fileid.get(table_name + "_" + column_name + "_index");
            return db.return_bfs_index(file_id);
        } else {
            System.out.println("Index does not exist");
        }
        return null;
    }

}