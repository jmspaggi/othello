import java.math.BigInteger;

import java.util.Arrays;
import java.util.Date;
import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.spaggiari.othello.logic.Cell;
import org.spaggiari.othello.logic.GameEngine;

import jdk.nashorn.internal.runtime.Context.ThrowErrorManager;

import java.util.Vector;
import java.util.ArrayList;

public class RandomGenerator {

  public static ArrayList<byte[]> poolBytes = new ArrayList<byte[]>();


  public static byte[] getBytesArray() {
    if (poolBytes.size() > 0) {
      byte[] element = poolBytes.remove(0);
      Arrays.fill(element, (byte) 0);
      return element;
    }
    return new byte[64];
  }

  public static void releaseBytesArray(byte[] array) {
    poolBytes.add(array);
  }

  public static byte countPieces(byte[][] board, byte player) {
    byte pieces = 0;
    for (int y = 0; y < 8; y++) {
      for (int x = 0; x < 8; x++) {
        if (board[x][y] == player)
          pieces++;
      }
    }
    return pieces;
  }

  /**
   * Generate random Reversi games. games: Number of games to generate. -1 = infinite
   */
  public static void generateRandomGames(int games, Connection connection) {
    Table table = null;
    try {
      if (connection != null) {
        TableName tableName = TableName.valueOf("games2");
        table = connection.getTable(tableName);
      }
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    byte[] gameSignature = new byte[13 * 60];
    while ((games == -1) || (games > 0)) {
      games = games - 1;
      byte[][] board = GameEngine.getStartingBoard(8);
      Cell nextMove = null;
      byte player = 0;
      byte moves = 0;
      boolean[] played = { true, true };
      while (played[0] || played[1]) {
        if (player == 1)
          player = 2;
        else
          player = 1;
        nextMove = GameEngine.randomChooseNextStep(board, player);
        played[player - 1] = nextMove != null;
        if (nextMove != null) {
          GameEngine.applyMove(board, nextMove.x, nextMove.y, player);
          byte[] gameRowKey = GameEngine.boardToRowKey(board);
          // System.out.println(Arrays.deepToString(board));
          // System.out.println(gameRowKey.length);
          System.arraycopy(gameRowKey, 0, gameSignature, moves * 13, gameRowKey.length);
          moves++;
        }
      }
      byte player1Score = countPieces(board, (byte) 1);
      byte player2Score = countPieces(board, (byte) 2);
      byte[] value = { player1Score, player2Score };
      Put put = new Put(gameSignature);
      put.addColumn("A".getBytes(), "A".getBytes(), value);
      try {
        table.put(put);
      } catch (IOException e) {
        e.printStackTrace();
      }
      // System.out.println(Arrays.toString(gameSignature) + Arrays.toString(value));
      GameEngine.releaseDoubleBytesArray(board);
      // System.out.println("Took " + (t2 - t1) + "ms.");
    }
    try {
      connection.close();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  public static void main(String[] args) {
    byte[][] board = GameEngine.getStartingBoard(8);
    for (int y = 0; y < 8; y++) {
      for (int x = 0; x < 8; x++) {
        board[x][y] = 2;
      }
    }
    byte[] rowKey = GameEngine.boardToRowKey(board);
    byte[][] boardAsPlayed = GameEngine.rowKeyToBoard(rowKey);
    /*
    System.out.println(Arrays.deepToString(board));
    System.out.println(Arrays.toString(rowKey));
    System.out.println(Arrays.deepToString(boardAsPlayed));
    System.out.println(new String(rowKey));
    System.out.println(toString(board));
    System.out.println(rowKey.length);
    // if (board[1][1] == 2) return;
    byte x = 4;
    byte y = 2;
    byte player = 1;
    System.out.println(isValidPlay(board, x, y, player));
    player = 2;
    System.out.println(isValidPlay(board, x, y, player));
    player = 1;
    for (y = 0; y < 8; y++) {
      for (x = 0; x < 8; x++) {
        System.out.print(isValidPlay(board, x, y, player) ? "o" : ".");
      }
      System.out.println();

    }
    */

    System.out.println(new Date() + " Starting");
    try {
      Configuration config = HBaseConfiguration.create();
      config.set("hbase.zookeeper.quorum", "192.168.57.100");
      Connection connection = ConnectionFactory.createConnection(config);
      long t1 = System.currentTimeMillis();
      int lines = 100000-82091;
      generateRandomGames(lines, connection);
      long t2 = System.currentTimeMillis();
      float flines = (float)lines;
      float rate = (t2 - t1) / flines;
      System.out.println(new Date() + " Generated " + lines + " in " + ((t2 - t1) / 1000) + " seconds, which is " + rate + " ms/line");
    } catch (Throwable t) {
      t.printStackTrace();
    }

  }

}
