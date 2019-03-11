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

import jdk.nashorn.internal.runtime.Context.ThrowErrorManager;

import java.util.Vector;
import java.util.ArrayList;

public class RandomGenerator {
  public static class Cell {
    public Cell () {
    }
    public Cell (byte x, byte y) {
      this.x = x;
      this.y = y;
    }
    public byte x;
    public byte y;
  }

  public static ArrayList<byte[][]> poolDoubleBytes = new ArrayList<byte[][]>();
  public static ArrayList<byte[]> poolBytes = new ArrayList<byte[]>();

  public static byte[][] getDoubleBytesArray() {
    if (poolDoubleBytes.size() > 0) {
      byte[][] element = poolDoubleBytes.remove(0);
      for (int i = 0; i < element.length; i++)
        Arrays.fill(element[i], (byte) 0);
      return element;
    }
    return new byte[8][8];
  }

  public static void releaseDoubleBytesArray(byte[][] array) {
    poolDoubleBytes.add(array);
  }

  public static byte[] getBytesArray() {
    if (poolDoubleBytes.size() > 0) {
      byte[] element = poolBytes.remove(0);
      Arrays.fill(element, (byte) 0);
      return element;
    }
    return new byte[64];
  }

  public static void releaseBytesArray(byte[] array) {
    poolBytes.add(array);
  }

  public static boolean isValidPlay(byte[][] board, byte x, byte y, byte player) {
    byte opponent = (byte) ((player % 2) + 1);
    // First, validate that we are inside the board.
    if ((x < 0) || (x > 7) || (y < 0) || (y > 7)) {
      return false; // We are trying to play outside the board.
    }
    // Make sure we are trying to play on an empty spot
    if (board[x][y] != 0) {
      return false; // There is already a piece where we are playing
    }
    // Now, piece should be places somewhere where it forms a line with another
    // piece, with opponent pieces in the middle.
    boolean flips = false;
    for (int direction = 0; (direction < 8) && !flips; direction++) {
      // There is 8 possible directions around that move.
      // For each of them we need to validate there is an adjacent piece AND
      // that it will flip some opponents.
      // To be a valid move, we need at least one valid direction.
      int stepX = 0;
      int stepY = 0;
      switch (direction) {
      case 0:
        stepX = -1;
        stepY = -1;
        break;
      case 1:
        stepX = 0;
        stepY = -1;
        break;
      case 2:
        stepX = +1;
        stepY = -1;
        break;
      case 3:
        stepX = -1;
        stepY = 0;
        break;
      case 4:
        stepX = +1;
        stepY = 0;
        break;
      case 5:
        stepX = -1;
        stepY = +1;
        break;
      case 6:
        stepX = 0;
        stepY = +1;
        break;
      case 7:
        stepX = +1;
        stepY = +1;
        break;
      default:
        System.out.println("Error. Skipping direction " + direction);
        break;
      }
      // First, look if this adjacent cell is inside the board.
      if ((x + stepX < 0) || (x + stepX > 7) || (y + stepY < 0) || (y + stepY > 7)) {
        continue;
      }
      // Second, look if we have an adjacent opponent piece.
      if (board[x + stepX][y + stepY] != opponent) {
        continue;
      }
      // Third make sure that on this line there is one of our own piece and no hole
      int step = 1;
      while (
          (x + step * stepX >= 0) &&
          (x + step * stepX < 8) &&
          (y + step * stepY >= 0) &&
          (y + step * stepY < 8) &&
          (board[x + step * stepX][y + step * stepY] == opponent)
          ) {
        step++;
      }
      flips = 
          (x + step * stepX >= 0) &&
          (x + step * stepX < 8) &&
          (y + step * stepY >= 0) &&
          (y + step * stepY < 8) &&
          (board[x + step * stepX][y + step * stepY] == player)
          ;
    }
    return flips;
  }

  public static boolean[] validDirections = new boolean[8]; // Stores which direction is valid to flip.

  // Almost a copy of isValidPlay, but update the board.
  public static boolean applyMove(byte[][] board, byte x, byte y, byte player) {
    if (!isValidPlay(board, x, y, player)) {
      return false;
    }
    board[x][y] = player;
    byte opponent = (byte) ((player % 2) + 1);
    // Now, piece should be places somewhere where it forms a line with another
    // piece, with opponent pieces in the middle.
    for (int direction = 0; (direction < 8); direction++) {
      // There is 8 possible directions around that move.
      // For each of them we need to validate there is an adjacent piece AND
      // that it will flip some opponents.
      // To be a valid move, we need at least one valid direction.
      int stepX = 0;
      int stepY = 0;
      switch (direction) {
      case 0:
        stepX = -1;
        stepY = -1;
        break;
      case 1:
        stepX = 0;
        stepY = -1;
        break;
      case 2:
        stepX = +1;
        stepY = -1;
        break;
      case 3:
        stepX = -1;
        stepY = 0;
        break;
      case 4:
        stepX = +1;
        stepY = 0;
        break;
      case 5:
        stepX = -1;
        stepY = +1;
        break;
      case 6:
        stepX = 0;
        stepY = +1;
        break;
      case 7:
        stepX = +1;
        stepY = +1;
        break;
      default:
        System.out.println("Error. Skipping direction " + direction);
        break;
      }
      // First, look if this adjacent cell is inside the board.
      if ((x + stepX < 0) || (x + stepX > 7) || (y + stepY < 0) || (y + stepY > 7)) {
        continue;
      }
      // Second, look if we have an adjacent opponent piece.
      if (board[x + stepX][y + stepY] != opponent) {
        continue;
      }
      // Third make sure that on this line there is one of our own piece and no hole
      int step = 1;
      while (
          (x + step * stepX >= 0) &&
          (x + step * stepX < 8) &&
          (y + step * stepY >= 0) &&
          (y + step * stepY < 8) &&
          (board[x + step * stepX][y + step * stepY] == opponent)
          ) {
        step++;
      }
      validDirections[direction] = 
          (x + step * stepX >= 0) &&
          (x + step * stepX < 8) &&
          (y + step * stepY >= 0) &&
          (y + step * stepY < 8) &&
          (board[x + step * stepX][y + step * stepY] == player)
          ;
    }

    for (byte direction = 0; (direction < 8); direction++) {
      if (validDirections[direction]) {
        // We have alread checked that on that direction there is a
        // valid line to flip.
        int stepX = 0;
        int stepY = 0;
        switch (direction) {
        case 0:
          stepX = -1;
          stepY = -1;
          break;
        case 1:
          stepX = 0;
          stepY = -1;
          break;
        case 2:
          stepX = +1;
          stepY = -1;
          break;
        case 3:
          stepX = -1;
          stepY = 0;
          break;
        case 4:
          stepX = +1;
          stepY = 0;
          break;
        case 5:
          stepX = -1;
          stepY = +1;
          break;
        case 6:
          stepX = 0;
          stepY = +1;
          break;
        case 7:
          stepX = +1;
          stepY = +1;
          break;
        default:
          System.out.println("Error. Skipping direction " + direction);
          break;
        }
        // Third make sure that on this line there is one of our own piece and no hole
        int step = 1;
        while ((x + step * stepX >= 0) &&
            (x + step * stepX < 8) &&
            (y + step * stepY >= 0) &&
            (y + step * stepY < 8) &&
            (board[x + step * stepX][y + step * stepY] == opponent)) {
          board[x + step * stepX][y + step * stepY] = player;
          step++;
        }
      }
    }
    return true;
  }

  // Buffers to store the X and Y of possible next steps.
  public static byte[] nextStepsX = new byte[64];
  public static byte[] nextStepsY = new byte[64];
  
  public static Cell chooseNextStep(byte[][] board, byte currentPlayer) {
    int options = 0;
    for (byte y = 0; y < 8; y++) {
      for (byte x = 0; x < 8; x++) {
        if (isValidPlay(board, x, y, currentPlayer)) {
          nextStepsX[options] = x;
          nextStepsY[options] = y;
          options++;
        }
      }
    }
    if (options == 0) {
      return null;
    }
    int random = ThreadLocalRandom.current().nextInt(0, options);
    return new Cell(nextStepsX[random], nextStepsY[random]);
  }

  public static String toString(byte[][] board) {
    StringBuffer buffer = new StringBuffer(73);
    for (int y = 0; y < 8; y++) {
      for (int x = 0; x < 8; x++) {
        switch (board[x][y]) {
        case 0:
          buffer.append(".");
          break;
        case 1:
          buffer.append("X");
          break;
        case 2:
          buffer.append("O");
          break;
        default:
          ;
        }
      }
      buffer.append("\n");
    }
    return buffer.toString();
  }

  public static byte[][] getStartingBoard() {
    byte[][] board = getDoubleBytesArray();
    board[3][3] = 1;
    board[4][4] = 1;
    board[3][4] = 2;
    board[4][3] = 2;
    return board;
  }

  // Return the board signature as a rowkey

  // Lenght can vary. Maximum length is 13 bytes;

  static private StringBuffer sBuffer = new StringBuffer(64);

  public static byte[] boardToRowKey(byte[][] board) {
    sBuffer.setLength(0);
    for (int y = 0; y < 8; y++) {
      for (int x = 0; x < 8; x++) {
        sBuffer.append(board[x][y]);
      }
    }
    BigInteger hexBoard = new BigInteger(sBuffer.toString(), 3);
    return new BigInteger(hexBoard.toString(16), 16).toByteArray();
  }

  public static byte[][] rowKeyToBoard(byte[] rowKey) {
    byte[][] board = getDoubleBytesArray();
    String hexBoard = new BigInteger(rowKey).toString(3);
    while (hexBoard.length() < 64) {
      hexBoard = "0" + hexBoard;
    }
    byte index = 0;
    for (int y = 0; y < 8; y++) {
      for (int x = 0; x < 8; x++) {
        board[x][y] = Byte.valueOf("" + hexBoard.charAt(index++));
      }
    }
    return board;
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
      byte[][] board = getStartingBoard();
      Cell nextMove = null;
      byte player = 0;
      byte moves = 0;
      boolean[] played = { true, true };
      while (played[0] || played[1]) {
        if (player == 1)
          player = 2;
        else
          player = 1;
        nextMove = chooseNextStep(board, player);
        played[player - 1] = nextMove != null;
        if (nextMove != null) {
          applyMove(board, nextMove.x, nextMove.y, player);
          byte[] gameRowKey = boardToRowKey(board);
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
      releaseDoubleBytesArray(board);
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
    byte[][] board = getStartingBoard();
    for (int y = 0; y < 8; y++) {
      for (int x = 0; x < 8; x++) {
        board[x][y] = 2;
      }
    }
    byte[] rowKey = boardToRowKey(board);
    byte[][] boardAsPlayed = rowKeyToBoard(rowKey);
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
