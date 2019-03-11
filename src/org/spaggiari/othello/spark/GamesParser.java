package org.spaggiari.othello.spark;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.spaggiari.othello.util.ByteArrayIterator;

import scala.Tuple2;

public class GamesParser {

  public static final TableName TABLE_NAME = TableName.valueOf("games");
  public static final byte[] COLUMN_FAMILY = Bytes.toBytes("A");
  public static final byte[] COLUMN_QUALIFIER = Bytes.toBytes("A");

  public static byte[] longToBytes(long l) {
    byte[] result = new byte[8];
    for (int i = 7; i >= 0; i--) {
      result[i] = (byte) (l & 0xFF);
      l >>= 8;
    }
    return result;
  }

  public static long bytesToLong(byte[] b) {
    long result = 0;
    for (int i = 0; i < 8; i++) {
      result <<= 8;
      result |= (b[i] & 0xFF);
    }
    return result;
  }

  @SuppressWarnings("serial")
  public static void processVersion1() {
    // SparkConf sc = new SparkConf().setAppName("ProcessTable").setMaster("local[2]");
    SparkConf sc = new SparkConf().setAppName("ProcessTable");
    JavaSparkContext jsc = new JavaSparkContext(sc);
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", "192.168.57.100");

    JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);

    Scan scan = new Scan();
    scan.setCaching(100);

    JavaRDD<Tuple2<ImmutableBytesWritable, Result>> data = hbaseContext.hbaseRDD(TABLE_NAME, scan);

    // Takes a row key a the columns and transform then of a list of boards plus results.
    // Results i a 3x128 bits array containing number_of_games + number_of_p1_cells + number_of_p2_cells

    PairFlatMapFunction<Iterator<Tuple2<ImmutableBytesWritable, Result>>, byte[], byte[]> mapToPair =
        new PairFlatMapFunction<Iterator<Tuple2<ImmutableBytesWritable, Result>>, byte[], byte[]>() {
          public Iterable<Tuple2<byte[], byte[]>> call(Iterator<Tuple2<ImmutableBytesWritable, Result>> input) throws Exception {
            ArrayList<Tuple2<byte[], byte[]>> ret = new ArrayList<Tuple2<byte[], byte[]>>();
            while (input.hasNext()) {
              Tuple2<ImmutableBytesWritable, Result> cell = input.next();
              ImmutableBytesWritable key = cell._1;
              Result resultValue = cell._2;
              // Value contains 2 bytes. First is score player 1. Second is score player 2.
              byte[] value = resultValue.getValue(COLUMN_FAMILY, COLUMN_QUALIFIER);
              byte scorePlayer1 = value[0];
              byte scorePlayer2 = value[1];
  
              int keyLength = key.getLength();
              byte[] outputValue = new byte[3 * 128];
              // We have to emit the 2 values in a 128 bits format, for later addition
              outputValue[127] = 1; // This contains only one board
              outputValue[127 + 128] = scorePlayer1;
              outputValue[127 + 128 + 128] = scorePlayer2;
  
              while (keyLength > 0) {
                /*
                byte[] boardSignature = new byte[13];
                System.arraycopy(keyBytes, keyOffset, boardSignature, 0, 13);
                keyLength -= 13;
                keyOffset += 13;
                ret.add(new Tuple2<byte[], byte[]>(boardSignature, outputValue));
                */
                ret.add(new Tuple2<byte[], byte[]>(new byte[] {1}, outputValue));
              }
            }
            return ret;
          }
        };

        PairFlatMapFunction<Iterator<Tuple2<ImmutableBytesWritable, Result>>, byte[], byte[]> mapToPair2 =
            new PairFlatMapFunction<Iterator<Tuple2<ImmutableBytesWritable, Result>>, byte[], byte[]>() {
              public Iterable<Tuple2<byte[], byte[]>> call(Iterator<Tuple2<ImmutableBytesWritable, Result>> input) throws Exception {
                ByteArrayIterator<Tuple2<byte[], byte[]>> ret = new ByteArrayIterator<Tuple2<byte[],byte[]>>();
                ret.setInput(input);
                return ret;
              }
            };
            
    Function2<byte[], byte[], byte[]> combine = new Function2<byte[], byte[], byte[]>() {

      public byte[] call(byte[] arg0, byte[] arg1) throws Exception {
        byte[] result = new byte[3 * 128];
        // First 128 bits are the number of games
        // Then 128 for the player1
        // Then 128 bits for the player2
        // We might not need 128 before a while, so for now use longs for calculations.
        long parties1 = arg0[127] + arg0[126] << 8 + arg0[125] << 16 + arg0[124] << 24;
        long parties2 = arg1[127] + arg1[126] << 8 + arg1[125] << 16 + arg1[124] << 24;
        long parties = parties1 + parties2;

        result[127] = (byte) (parties & 8); // Will not work, because of signed byte! Fuck it
        result[126] = (byte) ((parties >> 8) & 8); // Will not work, because of signed byte! Fuck it
        result[125] = (byte) ((parties >> 16) & 8); // Will not work, because of signed byte! Fuck it
        result[124] = (byte) ((parties >> 24) & 8); // Will not work, because of signed byte! Fuck it

        long player1Score1 = arg0[127 + 128] + arg0[126 + 128] << 8 + arg0[125 + 128] << 16 + arg0[124 + 128] << 24;
        long player1Score2 = arg1[127 + 128] + arg1[126 + 128] << 8 + arg1[125 + 128] << 16 + arg1[124 + 128] << 24;
        long player1Score = player1Score1 + player1Score2;

        result[127 + 128] = (byte) (player1Score & 8); // Will not work, because of signed byte! Fuck it
        result[126 + 128] = (byte) ((player1Score >> 8) & 8); // Will not work, because of signed byte! Fuck it
        result[125 + 128] = (byte) ((player1Score >> 16) & 8); // Will not work, because of signed byte! Fuck it
        result[124 + 128] = (byte) ((player1Score >> 24) & 8); // Will not work, because of signed byte! Fuck it

        long player2Score1 = arg0[127 + 128 + 128] + arg0[126 + 128 + 128] << 8 + arg0[125 + 128 + 128] << 16 + arg0[124 + 128 + 128] << 24;
        long player2Score2 = arg1[127 + 128 + 128] + arg1[126 + 128 + 128] << 8 + arg1[125 + 128 + 128] << 16 + arg1[124 + 128 + 128] << 24;
        long player2Score = player2Score1 + player2Score2;

        result[127 + 128 + 128] = (byte) (player2Score & 8); // Will not work, because of signed byte! Fuck it
        result[126 + 128 + 128] = (byte) ((player2Score >> 8) & 8); // Will not work, because of signed byte! Fuck it
        result[125 + 128 + 128] = (byte) ((player2Score >> 16) & 8); // Will not work, because of signed byte! Fuck it
        result[124 + 128 + 128] = (byte) ((player2Score >> 24) & 8); // Will not work, because of signed byte! Fuck it

        return result;
      }

    };

    JavaPairRDD<byte[], byte[]> moves = data.mapPartitionsToPair(mapToPair2).reduceByKey(combine);

    Function<Tuple2<byte[], byte[]>, Put> mapToPuts = new Function<Tuple2<byte[], byte[]>, Put>() {
      public Put call(Tuple2<byte[], byte[]> arg0) throws Exception {
        Put put = new Put(arg0._1);
        put.addColumn(COLUMN_FAMILY, COLUMN_QUALIFIER, arg0._2);
        return put;
      }
    };

    JavaRDD<Put> keyValuesPuts = moves.map(mapToPuts);
    
    // System.err.println("keyValuesPuts.count() = " + keyValuesPuts.count());


    hbaseContext.foreachPartition(keyValuesPuts,
      new VoidFunction<Tuple2<Iterator<Put>, Connection>>() {
        public void call(Tuple2<Iterator<Put>, Connection> t) throws Exception {
          Table table = t._2().getTable(TableName.valueOf("moves"));
          BufferedMutator mutator = t._2().getBufferedMutator(TableName.valueOf("moves"));
          while (t._1().hasNext()) {
            Put put = t._1().next();
            mutator.mutate(put);
          }

          mutator.flush();
          mutator.close();
          table.close();
        }
      });


    jsc.close();
  }

  public static void main(String[] args) {
    processVersion1();
  }

}
