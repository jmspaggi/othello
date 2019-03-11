package org.spaggiari.othello.test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

public class Walker implements Runnable {
  public static final Random random = new Random();
  
  // Define the tree size (related to the board size)
  public static final int WIDTH = 3; // Represents the number of possible moves at each step. 
  public static final int DEPTH = 4*4-4; // Represents the number of possible steps in a game. Should be about width*height - 4. Gives us 1 for a 4*4 board.
  // Took 2 minutes to run on my laptop, with 50 threads, for a 4*4 board with 3 possible moves at each step.

  // Size of the reserved subtree. The bigger, the less calls and stones, the smaller, the more concurrency..
  public static final int checkLevels = 3;

  // Table of all the completed and walking stones.
  public static final Hashtable<ByteArrayWrapper, String> stones = new Hashtable<ByteArrayWrapper, String>();

  // Simulates the latency to the "lock" system.
  public static final int delay = 10;

  protected String owner;

  public static void pause() {
    try {
      Thread.sleep(random.nextInt(delay));
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  /**
   * Replace a walking stone by a completion stone.
   * @param path
   * @param owner
   * @return
   */
  public static boolean swapStoneType(byte[] path, String owner) {
    pause();
    synchronized (Walker.class) {
      ByteArrayWrapper newStone = new ByteArrayWrapper(path);
      if (!stones.containsKey(newStone)) {
        System.err.println("Trying to swap a stone that doesn't exist.");
        return false;
      }
      stones.remove(newStone, owner);

      // So here, a walker is declaring a node has having all his children completed.
      // We have to check if this is correct or a bug. This will not be required in the final implementation. 
      // Here just for validation of the logic.

      // First, means the marked node should be empty or owned by the owner
      ByteArrayWrapper arrayPath = new ByteArrayWrapper(path);
      String previousOwner = stones.get(arrayPath);
      if (previousOwner != null) {
        if (previousOwner.startsWith("COMPLETED)")) {
          // We are trying to mark something as completed while it has already been done.
          System.err.println("Marking completed by " + owner + " when " + previousOwner + " already did it");
        } else {
          if (!previousOwner.equals(owner)) {
            // Trying to mark as completed while someone else is still working on that branch
            System.err.println("Trying to mark as completed for a different owner (In swap) " + previousOwner + " vs " + owner);
            System.err.println(new ByteArrayWrapper(path));
          }
        }
      }

      stones.put(new ByteArrayWrapper(path), "COMPLETED BY " + owner);
    }
    return true;
  }

  /**
   * Put a walking stone in the tree. Means this owner reserves that branch for now.
   * @param path
   * @param owner
   * @return true is the branch has been successfully reserved.
   */
  public static boolean putWalkingStone(byte[] path, String owner) {
    pause();
    boolean response = false;
    synchronized (Walker.class) {
      // We are simulating the transaction operation by synchronizing on the Hashmap
      ByteArrayWrapper newStone = new ByteArrayWrapper(path.clone());
      if (stones.containsKey(newStone)) {
        response = owner.equals(stones.get(newStone));
        if (response) {
          // Is it not expected to reserve a path we already own. 
          // However, it's normal to try to reserve a path that someone else might own.
          System.err.println(owner + " is trying to reserve a path it aleady owns => " + newStone.toString());
        }
      } else {
        stones.put(newStone, owner);
        response = true;
      }
    }
    return response;
  }

  /**
   * Check if there is an existing walking stone for given owner
   * @param path
   * @param owner
   * @return
   */
  public static boolean isWalkingStone(byte[] path, String owner) {
    ByteArrayWrapper newStone = new ByteArrayWrapper(path);
    synchronized (Walker.class) {
      return (stones.get(newStone) != null) && stones.get(newStone).equals(owner);
    }
  }

  /**
   * Remove a walking stone for a specific owner.
   * @param path
   * @param owner
   * @return
   */
  public static boolean removeWalkingStone(byte[] path, String owner) {
    pause();
    synchronized (Walker.class) {
      ByteArrayWrapper newStone = new ByteArrayWrapper(path);
      if (!stones.containsKey(newStone)) {
        System.err.println("Not existing in removeWalkingStone");
        return false;
      }
      stones.remove(newStone, owner);
      return true;
    }
  }

  /**
   * Mark a branch as completed. 
   * @param path
   * @param owner
   * @return
   */
  public static boolean putCompletedStone(byte[] path, String owner) {
    pause();
    synchronized (Walker.class) {
      // So here, a walker is declaring a node has having all his children completed.
      // We have to check if this is correct or a bug.
      // First, means the marked node should be empty or owned by the owner
      ByteArrayWrapper arrayPath = new ByteArrayWrapper(path);
      String previousOwner = stones.get(arrayPath);
      if (previousOwner != null) {
        if (previousOwner.startsWith("COMPLETED)")) {
          // We are trying to mark something as completed while it has already been done.
          System.err.println("Marking completed by " + owner + " when " + previousOwner + " already did it");
        } else {
          if (!previousOwner.equals(owner)) {
            // Trying to mark as completed while someone else is still working on that branch
            System.err.println(owner + " trying to mark as completed when " + previousOwner + " already reserved that stone.");
            return false;
          } else {
            System.err.println("Stepping on my own foot (Putting completion stone instead of swapping it. " + Arrays.toString(path));
          }
        }
      }
      stones.put(new ByteArrayWrapper(path), "COMPLETED BY " + owner);
      // System.out.println(new ByteArrayWrapper(path) +  " COMPLETED BY " + owner);
    }
    return true;
  }


  public static boolean isCompleted(byte[] path) {
    String stone = stones.get(new ByteArrayWrapper(path));
    return (stone != null) && (stone.startsWith("COMPLETED")); 
  }

  /**
   * Walk the tree... Main logic. This should not print anything on the error output. If it does, means logic is not good.
   * @param currentPath
   * @param currentDepth
   * @return the number of levels we have to step down.
   */
  public int walk(byte[] currentPath, int currentDepth) {
    // Implementation detail. Since we are recursive we clone the path, to not mess with the given parameter.
    currentPath = currentPath.clone();

    // First we need to figure if we reached a leaf.
    if (currentDepth == DEPTH) {
      // Mark it as reached/processed.
      putCompletedStone(currentPath, owner);
      return 1; // Return 1, because we don't have to step down more than just one level
      // In the Othello implementation, this is where we will have the final board with all the steps. 
      // This is where we will update all the metrics, search for duplication, etc.
    }

    // We put walking stones every checkLevels levels deep. Validate if we have to. This includes lvl 0.
    if (currentDepth % checkLevels == 0) {
      boolean reservedPath = putWalkingStone(currentPath, owner);
      if (!reservedPath) {
        // We are not able to reserve the path where we are. Probably because someone is already walking it.
        // 2 options here.
        // - We are at level zero. Means another walker already just started and is finding its path. Just exit. 
        // - We are in the tree. Means we have a walking stone somewhere behind us that is protecting us. Go back to that stone.
        return Math.min(checkLevels, currentDepth); // Return checkLevels, to say we have to step down checkLevels levels.
      } else {
        // If we already have a walking stone checkLevels below, we can remote it.
        if (currentDepth >= checkLevels) {
          // Build what was the path checkLevels below.
          byte[] unlockPath = currentPath.clone();
          for (int i = currentDepth; i >= currentDepth - checkLevels; i--) {
            unlockPath[i] = 127;
          }
          if (isWalkingStone(unlockPath, owner))
            removeWalkingStone(unlockPath, owner);
          else {
            // There was no previous walking stone. This is not normal.
            System.err.println("We are missing our previous walking stone.");
            return currentDepth + 1;
          }
        }
      }
    }

    // If we reach that points it means 2 things.
    // - We didn't reach a leaf
    // - We are protected by a walking stone.

    // Among all our next possible steps, randomly pick-up one that is not done yet.
    boolean[] toDo = new boolean[WIDTH];
    boolean allDone = false;
    while (!allDone) {
      allDone = true;
      for (byte i = 0; i < toDo.length; i++) {
        currentPath[currentDepth] = i;
        toDo[i] = !isCompleted(currentPath);
        allDone = allDone && !toDo[i];
      }
      // Restore the currentPath the way it was.
      currentPath[currentDepth] = 127;
      if (allDone) {
        // All the children are fully processed. We can mark current place has processed and step down.
        if ((currentDepth % checkLevels == 0) && (currentDepth > 0)) {
          // We are at a level where we are supposed to have put a walking stone. We just need to swap it.
          if (!swapStoneType(currentPath, owner)) {
            System.err.println(Arrays.toString(currentPath));
          }
          // Try to put a walking stone checkLevels below. If we succeed, we can continue. Else, we just exit
          // (One possible implementation is to keep trying every checkLevels down)
          byte[] reservedPath = new byte[DEPTH];
          Arrays.fill(reservedPath, (byte)127);
          int tentativeNewDepth = currentDepth - checkLevels;
          System.arraycopy(currentPath, 0, reservedPath, 0, tentativeNewDepth);
          if (putWalkingStone(reservedPath, owner))
            return checkLevels;
          else
              return currentDepth + 1;
        } else {
          // We are still below our walking stone. Just just need to mark current node as completed and step down.
          if (!putCompletedStone(currentPath, owner)) {
            System.err.println(currentDepth);
            System.err.println(Arrays.toString(currentPath));
          }
          return 1;
        }
      }
      // Select a random destination
      byte randomDirection = (byte) Math.round(random.nextInt(WIDTH));
      while (!toDo[randomDirection]) {
        randomDirection = (byte) Math.round(random.nextInt(WIDTH));
      }
      currentPath[currentDepth] = randomDirection;
      // Walk down a random path.
      int stepsDown = walk(currentPath, currentDepth + 1);
      currentPath[currentDepth] = 127;
      if (stepsDown > 1) // We have been asked to go down, so we do.
      {
        return stepsDown-1;
      }
      // Else we continue with the next child.
    }
    System.err.println("We should not reach that point. Ever.");
    return 1;
  }

  public void run() {
    byte[] path = new byte[DEPTH]; // Initialize the path based on the possible deepest path.
    Arrays.fill(path, (byte) 127);
    walk(path, 0);
    System.out.println(owner + " is done.");
  }

  public Walker() {
    owner = UUID.randomUUID().toString();
  }

  public Walker(String owner) {
    this.owner = owner;
  }

  /**
   * Implement the walker logic in a stand alone test application. Goal is to validate that walkers are able to cover the entire tree without stepping
   * on each others. This test application takes 3 parameters. The number of walkers, the depth of the tree and the width of the tree. All hard coded
   * of course ;) Goal is really to have a quick and dirty way to test the logic.
   * @param args
   * @throws IOException 
   */
  public static void main(String[] args) throws IOException {
    int nbWalkers = 50;
    Walker[] walkers = new Walker[nbWalkers];
    Thread[] threads = new Thread[nbWalkers];
    for (int i = 0; i < nbWalkers; i++) {
      walkers[i] = new Walker("" + (char)(65+i));
      threads[i] = new Thread(walkers[i]);
    }
    long time1 = System.currentTimeMillis();
    byte[] firstNode = new byte[DEPTH];
    Arrays.fill(firstNode, (byte) 127);
    try {
      // Keep re-launching walkers until firstNode is marked as completed.
      // Walkers might stop running when they have to start back from the top of the tree. It's expected.
      while (!isCompleted(firstNode)) {
        for (int i = 0; i < nbWalkers; i++) {
          if (!threads[i].isAlive()){
            System.out.println("Starting " + walkers[i].owner);
            threads[i] = new Thread(walkers[i]);
            threads[i].start();
            Thread.sleep(500);
          }
        }
        Thread.sleep(100);
      }
      System.out.println("All nodes completed.");
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    long time2 = System.currentTimeMillis();

    int[] workers = new int[nbWalkers];
    for (Map.Entry<ByteArrayWrapper, String> entry : stones.entrySet()) {
      String value = entry.getValue();
      workers[((byte)value.charAt(value.length() - 1))-65]++; // Count who did what.
    }
    System.out.println(stones.size());
    System.out.println(Arrays.toString(workers));

    System.out.println(time2 - time1);
  }
}
