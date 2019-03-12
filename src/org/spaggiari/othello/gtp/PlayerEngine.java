package org.spaggiari.othello.gtp;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.spaggiari.othello.logic.Cell;
import org.spaggiari.othello.logic.GameEngine;

public class PlayerEngine implements Runnable {
  public final static char CR = (char) 0x0D;
  public final static char LF = (char) 0x0A;
  public final static String CRLF = "" + CR + LF;

  public final static String PROTOCOL_VERSION = "protocol_version";
  public final static String NAME = "name";
  public final static String VERSION = "version";
  public final static String KNOW_COMMAND = "known_command";
  public final static String LIST_COMMANDS = "list_commands";
  public final static String QUIT = "quit";
  public final static String BOARDSIZE = "boardsize";
  public final static String CLEAR_BOARD = "clear_board";
  public final static String KOMI = "komi";
  public final static String PLAY = "play";
  public final static String GENMOVE = "genmove";
  public final static String SET_GAME = "set_game";
  public final static String LIST_GAMES = "list_games";
  public final static String FINAL_SCORE = "final_score";
  public final static String SHOWBOARD = "showboard";

  public final static File logFile = new File("/tmp/gtp.log");
  public static BufferedWriter logWriter = null;

  protected BufferedReader br = null;
  protected BufferedWriter bw = null;
  protected boolean shouldExit = false;
  public static int boardSize = 0; // Ugly!
  protected byte[][] board = null;
  protected final Vector<byte[][]> moves = new Vector<>();

  public static void logWriter(String string) throws IOException {
    logWriter.write(string);
    logWriter.flush();
  }

  public PlayerEngine(InputStream is, OutputStream os) throws IOException {
    br = new BufferedReader(new InputStreamReader(is));
    bw = new BufferedWriter(new OutputStreamWriter(os));
    logWriter = new BufferedWriter(new FileWriter(logFile));
  }

  public void processProtocolVersion() throws IOException {
    bw.write("= 2" + LF + LF);
  }

  public void processName() throws IOException {
    bw.write("= Spaggiari" + LF + LF);
  }

  public void processVersion() throws IOException {
    bw.write("= 0.0.1" + LF + LF);
  }

  public void processNotImplemented() throws IOException {
    bw.write("? not impletement" + LF + LF);
  }

  public void processListCommands() throws IOException {
    bw.write("= " + PROTOCOL_VERSION + LF + NAME + LF + VERSION + LF + KNOW_COMMAND + LF + LIST_COMMANDS + LF + QUIT + LF + BOARDSIZE + LF
        + CLEAR_BOARD + LF + KOMI + LF + PLAY + LF + GENMOVE + LF + SET_GAME + LF + LIST_GAMES + LF + FINAL_SCORE + LF + SHOWBOARD + LF + LF);
  }

  public void processQuit() throws IOException {
    bw.write("= " + LF + LF);
    shouldExit = true;
  }

  public void processBoardSize(String parameter) throws IOException {
    if ("6".equals(parameter)) {
      bw.write("= " + LF + LF);
      boardSize = 6;
      return;
    } else {
      if ("8".equals(parameter)) {
        bw.write("= " + LF + LF);
        boardSize = 8;
        return;
      }
    }
    bw.write("? board size not managed");
  }

  public void processClearBoard() throws IOException {
    bw.write("= " + LF + LF);
    board = new byte[boardSize][boardSize];
    if (boardSize == 8) {
      board[3][3] = 2;
      board[4][3] = 1;
      board[3][4] = 1;
      board[4][4] = 2;
    } else {
      if (boardSize == 6) {
        board[2][2] = 2;
        board[3][2] = 1;
        board[2][3] = 1;
        board[3][3] = 2;
      }
    }
  }

  public static byte[][] copy(byte[][] src) {
    byte[][] dst = new byte[src.length][];
    for (int i = 0; i < src.length; i++) {
      dst[i] = Arrays.copyOf(src[i], src[i].length);
    }
    return dst;
  }

  public void processPlay(String parameter) throws IOException {
    // @TODO assuming all plays are valid. Need to implement validation
    String strColor = parameter.substring(0, parameter.indexOf(" "));
    String strCell = parameter.substring(parameter.indexOf(" ") + 1);
    byte playerID = 0;
    if ("white".equals(strColor)) {
      playerID = 2;
    } else {
      if ("black".equals(strColor)) {
        playerID = 1;
      } else {
        bw.write("? unknown player color" + LF + LF);
        shouldExit = true;
        return;
      }
    }
    // logWriter("Player for " + playerID + " in " + strCell);
    try {
      byte column = (byte) ((byte) strCell.charAt(0) - 65);
      byte line = (byte) (((byte) strCell.charAt(1)) - 49);
      logWriter("Playing in " + column + "x" + line);
      GameEngine.applyMove(board, column, line, playerID);
      bw.write("= " + LF + LF);
      moves.addElement(copy(board));
    } catch (Throwable t) {
      logWriter(t.getMessage());
      logWriter(t.getStackTrace()[0].toString());
      bw.write("? Failed to apply the play" + LF + LF);
      shouldExit = true;
    }
  }

  public void processGenMove(String parameter) throws IOException {
    try {
      String strColor = parameter;
      byte playerID = 0;
      if ("white".equals(strColor)) {
        playerID = 2;
      } else {
        if ("black".equals(strColor)) {
          playerID = 1;
        } else {
          bw.write("? unknown player color" + LF + LF);
          shouldExit = true;
          return;
        }
      }
      // Build board ID
      // Get all next moves from HBase.
      // Return the best. If all are losts, see if there is one that was never played and return.
      // If no moves, return randomly.
      Cell cell = GameEngine.randomChooseNextStep(board, playerID);
      bw.write("= " + (char) (65 + cell.x) + (cell.y + 1) + LF + LF);
      GameEngine.applyMove(board, cell.x, cell.y, playerID);
      moves.addElement(copy(board));
    } catch (Throwable t) {
      logWriter(t.getStackTrace()[0].toString());
      logWriter(t.getMessage());
    }
  }

  public void processSetGame(String parameter) throws IOException {
    if (!"Othello".equals(parameter)) {
      bw.write("? Don't know how to play " + parameter + LF + LF);
      return;
    }
    bw.write("= " + LF + LF);
  }

  public void processListGames() throws IOException {
    bw.write("= Othello" + LF + LF);
  }

  public void run() {
    try {
      while (!shouldExit) {
        String command = br.readLine();
        String parameter = null;
        logWriter.write("Received " + command + CRLF);
        logWriter.flush();
        if (command == null)
          break;
        if (command.indexOf(" ") > 0) {
          parameter = command.substring(command.indexOf(" ") + 1);
          command = command.substring(0, command.indexOf(" "));
        }
        switch (command) {
        case PROTOCOL_VERSION:
          processProtocolVersion();
          break;
        case NAME:
          processName();
          break;
        case VERSION:
          processVersion();
          break;
        case KNOW_COMMAND:
          processNotImplemented();
          break;
        case LIST_COMMANDS:
          processListCommands();
          break;
        case QUIT:
          processQuit();
          break;
        case BOARDSIZE:
          processBoardSize(parameter);
          break;
        case CLEAR_BOARD:
          processClearBoard();
          break;
        case KOMI:
          processNotImplemented();
          break;
        case PLAY:
          processPlay(parameter);
          break;
        case GENMOVE:
          processGenMove(parameter);
          break;
        case SET_GAME:
          processSetGame(parameter);
          break;
        case LIST_GAMES:
          processListGames();
          break;
        default:
          // Sent back error unknown command
          shouldExit = true;
          bw.write("? Unknown command" + LF + LF);
          break;
        }
        bw.flush();
      }
      // Validate if we have completed a game. If we did, store it
      // A completed game is a boad where neither the black of the white player can play.
      logWriter("Remaining black move: " + GameEngine.randomChooseNextStep(board, (byte) 1));
      logWriter("Remaining white move: " + GameEngine.randomChooseNextStep(board, (byte) 2));
      if ((GameEngine.randomChooseNextStep(board, (byte) 1) == null) && (GameEngine.randomChooseNextStep(board, (byte) 2) == null)) {
        logWriter("We have completed a game. We need to record it." + LF);
        byte[] gameSignature = new byte[moves.size()*13];
        int pos = 0;
        logWriter(GameEngine.toString(moves.elementAt(0)));
        logWriter("" + LF);
        for (Iterator<byte[][]> iterator = moves.iterator(); iterator.hasNext();) {
          byte[] boardSignature = GameEngine.boardToRowKey(iterator.next());
          logWriter(Arrays.toString(boardSignature));
          logWriter("" + LF);
          System.arraycopy(boardSignature, 0, gameSignature, pos*13 + (13 - boardSignature.length), boardSignature.length);
          pos++;
        }
        logWriter("Game signature: " + Arrays.toString(gameSignature));
        byte[] revert = new byte[13];
        System.arraycopy(gameSignature, 0, revert, 0, 13);
        logWriter("" + LF);
        logWriter(Arrays.toString(revert));
        logWriter("" + LF);
        logWriter(GameEngine.toString(GameEngine.rowKeyToBoard(revert)));
        logWriter("" + LF);
        try {
          logWriter("Writting game into HBase" + LF);
          Configuration config = HBaseConfiguration.create();
          config.set("hbase.zookeeper.quorum", "192.168.57.100");
          Connection connection = ConnectionFactory.createConnection(config);
          TableName tableName = TableName.valueOf("played");
          Table table = connection.getTable(tableName);
          Put put = new Put(gameSignature);
          put.addColumn("@".getBytes(), "@".getBytes(), "@".getBytes());
          table.put(put);
          table.close();
          connection.close();
          logWriter("Successfully wrote into HBase" + LF);
        } catch (IOException e) {
          // TODO Auto-generated catch block
          logWriter(e.getMessage());
          logWriter(e.getStackTrace()[0].toString());
          logWriter.flush();
          e.printStackTrace();
        }
      }
      logWriter.flush();
    } catch (Exception e) {
      try {
        logWriter.write(e.getMessage());
        logWriter.write(e.getStackTrace()[0].toString());
        logWriter.flush();
      } catch (IOException e1) {
        // TODO Auto-generated catch block
        e1.printStackTrace();
      }
    }
  }

  public static void main(String[] args) {
    PlayerEngine engine = null;
    try {
      engine = new PlayerEngine(System.in, System.out);
    } catch (IOException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }
    Thread thread = new Thread(engine);
    thread.start();
    while (thread.isAlive()) {
      try {
        Thread.sleep(1000);
        Thread.yield();
      } catch (InterruptedException e) {
      }
    }
  }

}
