package org.spaggiari.othello.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

import scala.Tuple2;

public class ByteArrayIterator<Type> implements Iterable<Type> {

  private ArrayList<ByteArrayInputStream> storedBIS = new ArrayList<ByteArrayInputStream>();
  protected int index = 0;
  Iterator<Tuple2<ImmutableBytesWritable, Result>> input = null;
  ArrayList<Type> buffer = new ArrayList<Type>();
  public static final TableName TABLE_NAME = TableName.valueOf("games");
  public static final byte[] COLUMN_FAMILY = Bytes.toBytes("A");
  public static final byte[] COLUMN_QUALIFIER = Bytes.toBytes("A");

  public ByteArrayIterator() {
  }

  public void setInput(Iterator<Tuple2<ImmutableBytesWritable, Result>> input) {
    this.input = input;

  }

  public void append(ByteArrayInputStream bis) {
    storedBIS.add(bis);
  }

  public Iterator<Type> iterator() {
    Iterator<Type> it = new Iterator<Type>() {

      public boolean hasNext() {
        return buffer.size() > 0 || input.hasNext();
      }

      public Type next() {
        if (buffer.size() > 0)
          return buffer.remove(0);
        Tuple2<ImmutableBytesWritable, Result> cell = input.next();
        ImmutableBytesWritable key = cell._1;
        Result resultValue = cell._2;
        // Value contains 2 bytes. First is score player 1. Second is score player 2.
        byte[] value = resultValue.getValue(COLUMN_FAMILY, COLUMN_QUALIFIER);
        byte scorePlayer1 = value[0];
        byte scorePlayer2 = value[1];

        byte[] keyBytes = key.get();
        int keyOffset = key.getOffset();
        int keyLength = key.getLength();
        byte[] outputValue = new byte[3 * 128];
        // We have to emit the 2 values in a 128 bits format, for later addition
        outputValue[127] = 1; // This contains only one board
        outputValue[127 + 128] = scorePlayer1;
        outputValue[127 + 128 + 128] = scorePlayer2;

        while (keyLength > 0) {
          byte[] boardSignature = new byte[13];
          System.arraycopy(keyBytes, keyOffset, boardSignature, 0, 13);
          keyLength -= 13;
          keyOffset += 13;
          buffer.add((Type)new Tuple2<byte[], byte[]>(boardSignature, outputValue));
        }
        return buffer.remove(0);
      }

      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
    return it;
  }

}