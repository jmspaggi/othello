package org.spaggiari.othello.logic;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

import org.spaggiari.othello.gtp.PlayerEngine;

public class GameEngine {
  public static boolean isValidPlay(byte[][] board, byte x, byte y, byte player) {
    byte opponent = (byte) ((player % 2) + 1);
    // First, validate that we are inside the board.
    if ((x < 0) || (x > board.length - 1) || (y < 0) || (y > board[0].length - 1)) {
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
      if ((x + stepX < 0) || (x + stepX > board.length - 1) || (y + stepY < 0) || (y + stepY > board[0].length - 1)) {
        continue;
      }
      // Second, look if we have an adjacent opponent piece.
      if (board[x + stepX][y + stepY] != opponent) {
        continue;
      }
      // Third make sure that on this line there is one of our own piece and no hole
      int step = 1;
      while ((x + step * stepX >= 0) && (x + step * stepX < board.length) && (y + step * stepY >= 0) && (y + step * stepY < board[0].length)
          && (board[x + step * stepX][y + step * stepY] == opponent)) {
        step++;
      }
      flips = (x + step * stepX >= 0) && (x + step * stepX < board.length) && (y + step * stepY >= 0) && (y + step * stepY < board[0].length)
          && (board[x + step * stepX][y + step * stepY] == player);
    }
    return flips;
  }
  
  public static boolean[] validDirections = new boolean[8]; // Stores which direction is valid to flip.

  // Almost a copy of isValidPlay, but update the board.
  public static boolean applyMove(byte[][] board, byte x, byte y, byte player) {
    if (!GameEngine.isValidPlay(board, x, y, player)) {
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
      if ((x + stepX < 0) || (x + stepX > board.length - 1) || (y + stepY < 0) || (y + stepY > board[0].length - 1)) {
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
          (x + step * stepX < board.length) &&
          (y + step * stepY >= 0) &&
          (y + step * stepY < board[0].length) &&
          (board[x + step * stepX][y + step * stepY] == opponent)
          ) {
        step++;
      }
      validDirections[direction] = 
          (x + step * stepX >= 0) &&
          (x + step * stepX < board.length) &&
          (y + step * stepY >= 0) &&
          (y + step * stepY < board[0].length) &&
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
            (x + step * stepX < board.length) &&
            (y + step * stepY >= 0) &&
            (y + step * stepY < board[0].length) &&
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

  public static Cell randomChooseNextStep(byte[][] board, byte currentPlayer) {
    int options = 0;
    for (byte y = 0; y < board[0].length; y++) {
      for (byte x = 0; x < board.length; x++) {
        if (GameEngine.isValidPlay(board, x, y, currentPlayer)) {
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
  
  
  public static ArrayList<byte[][]> poolDoubleBytes = new ArrayList<byte[][]>();

  public static byte[][] getDoubleBytesArray() {
    return getDoubleBytesArray(PlayerEngine.boardSize);
  }

  public static byte[][] getDoubleBytesArray(int boardSize) {
    if (poolDoubleBytes.size() > 0) {
      byte[][] element = poolDoubleBytes.remove(0);
      for (int i = 0; i < element.length; i++)
        Arrays.fill(element[i], (byte) 0);
      return element;
    }
    return new byte[boardSize][boardSize];
  }

  public static void releaseDoubleBytesArray(byte[][] array) {
    poolDoubleBytes.add(array);
  }

  public static byte[][] rowKeyToBoard(byte[] rowKey) {
    byte[][] board = getDoubleBytesArray();
    String hexBoard = new BigInteger(rowKey).toString(3);
    while (hexBoard.length() < PlayerEngine.boardSize*PlayerEngine.boardSize) {
      hexBoard = "0" + hexBoard;
    }
    byte index = 0;
    for (int y = 0; y < board[0].length; y++) {
      for (int x = 0; x < board.length; x++) {
        board[x][y] = Byte.valueOf("" + hexBoard.charAt(index++));
      }
    }
    return board;
  }
  
  public static void main(String args[]) {
    PlayerEngine.boardSize = 6;
    byte[][] board = getStartingBoard(6);
    // byte[] key = {5, -81, -16, -56, 124, 0, 0, 0, 0, 0, 0, 0, 0};
    System.out.println(Arrays.toString(boardToRowKey(board)));
    System.out.println();
    System.out.println(toString(board));
    System.out.println();
    System.out.println(toString(rowKeyToBoard(boardToRowKey(board))));
  }

  public static byte[][] getStartingBoard(int boardSize) {
    byte[][] board = getDoubleBytesArray(boardSize);
    if (boardSize == 8) {
      board[3][3] = 1;
      board[4][4] = 1;
      board[3][4] = 2;
      board[4][3] = 2;
    } else {
      board[2][2] = 1;
      board[3][3] = 1;
      board[2][3] = 2;
      board[3][2] = 2;
    }
    return board;
  }


  public static String toString(byte[][] board) {
    StringBuffer buffer = new StringBuffer(73);
    for (int y = 0; y < board[0].length; y++) {
      for (int x = 0; x < board.length; x++) {
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

  static private StringBuffer sBuffer = new StringBuffer(64);

  public static byte[] boardToRowKey(byte[][] board) {
    sBuffer.setLength(0);
    for (int y = 0; y < board[0].length; y++) {
      for (int x = 0; x < board.length; x++) {
        sBuffer.append(board[x][y]);
      }
    }
    BigInteger hexBoard = new BigInteger(sBuffer.toString(), 3);
    return new BigInteger(hexBoard.toString(16), 16).toByteArray();
  }
}
