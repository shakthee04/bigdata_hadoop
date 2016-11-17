package com.mindtree.skillspeed.module_9_assignment;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
/**
 * @author M1032938
 *
 */
public class DeleteData {

   public static void main(String[] args) throws IOException {

      // Instantiating Configuration class
      Configuration conf = HBaseConfiguration.create();

      // Instantiating HTable class
      HTable table = new HTable(conf, "employee");

      // Instantiating Delete class
      Delete delete = new Delete(Bytes.toBytes("row1"));
      delete.deleteColumn(Bytes.toBytes("personal"), Bytes.toBytes("name"));
     
      // deleting the data
      table.delete(delete);

      // closing the HTable object
      table.close();
      System.out.println("data deleted.....");
   }
}
