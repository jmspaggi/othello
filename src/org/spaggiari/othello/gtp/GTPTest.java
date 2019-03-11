package org.spaggiari.othello.gtp;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Date;

public class GTPTest {
  
  public final static char CR  = (char) 0x0D;
  public final static char LF  = (char) 0x0A; 

  public final static String CRLF  = "" + CR + LF;     // "" forces conversion to string
  
  public static int boardSize = 8;


  public static void main(String[] args) {
    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
    BufferedWriter writer = null;
    try {
      // create a temporary file
      File logFile = new File("/tmp/gtp.log");

      writer = new BufferedWriter(new FileWriter(logFile));
      writer.write("Version GTPTest 0.2 " + new Date() + "\n");
      boolean exit = false;
      while(!exit) {
        String line = reader.readLine();
        String response = null;
        if (line != null) {
          writer.write("Received: " + line);
          response = null;
          if ("protocol_version".equals(line)) {
            response = "= 2" + LF + LF;
          }
          if ("name".equals(line)) {
            response = "= Spaggiari" + LF + LF;
          }
          if ("version".equals(line)) {
            response = "= 1" + LF + LF;
          }
          if ("list_commands".equals(line)) {
            response = "= protocol_version" + LF + "name" + LF + "version" + LF + "known_command" + LF + "list_commands" + LF + "quit" + LF + "boardsize" + LF + "clear_board" + LF + "komi" + LF + "play" + LF + "genmove" + LF + "set_game" + LF + "list_games" + LF + "final_score" + LF + "cputime" + LF +"showboard" + LF + "time_settings" + LF + "time_left" + LF + LF;
          }
          if ("quit".equals(line)) {
            response = "= " + LF + LF;
            exit = true;
          }
          if ("list_games".equals(line)) {
            response = "= Othello" + LF + LF;
          }
          if ("set_game Othello".equals(line)) {
            response = "= " + LF + LF;
          }
          if ("boardsize 6".equals(line)) {
            boardSize = 6;
            response = "= " + LF + LF;
          }
          if ("boardsize 8".equals(line)) {
            boardSize = 8;
            response = "= " + LF + LF;
          }
          if ("clear_board".equals(line)) {
            response = "= " + LF + LF;
          }
          if ("play black D5".equals(line)) {
            response = "= " + LF + LF;
          }
          if ("genmove white".equals(line)) {
            response = "= C5" + LF + LF;
          }
          if (response != null) {
            System.out.print(response);
            System.out.flush();
            writer.write(" Replied " + response);
          }
          writer.flush();
        }
      }
    } catch (Exception e) {
      try {
        writer.write(e.getMessage());
      } catch (IOException e1) {
        // TODO Auto-generated catch block
        e1.printStackTrace();
      }
      e.printStackTrace();
    } finally {
      try {
        // Close the writer regardless of what happens...
        writer.close();
      } catch (Exception e) {
      }
    }
  }

}
