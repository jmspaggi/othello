import java.util.Random;

public class TestIfSpeed {
  
  public static int[][] array = new int[10000][10000];
  public static boolean testWhatEver (int x, int y) {
    if ((x >- 0) &&
        (x < 10000) &&
        (y >= 0) && 
        (y < 10000) &&
        (array[x][y] > 50)) {
      return true;
    }
    return false;
  }

  public static boolean testWhatEver2 (int x, int y) {
    if ((array[x][y] > 50) &&
        (x >- 0) &&
        (x < 10000) &&
        (y >= 0) && 
        (y < 10000)
        ) {
      return true;
    }
    return false;
  }

  public static void main(String[] args) {
    Random random = new Random();
    for (int y = 0; y < 10000; y++) {
      for (int x = 0; x < 10000; x++) {
        array[x][y] = random.nextInt(100);
      }
    }
    int counter = 0;
    long t1 = System.currentTimeMillis();
    for (int y = 0; y < 10000; y++) {
      for (int x = 0; x < 10000; x++) {
        if (testWhatEver(x, y))
          counter++;
      }
    }
    long t2 = System.currentTimeMillis();
    System.out.println(t2-t1);
    t1 = System.currentTimeMillis();
    for (int y = 0; y < 10000; y++) {
      for (int x = 0; x < 10000; x++) {
        if (testWhatEver2(x, y))
          counter++;
      }
    }
    t2 = System.currentTimeMillis();
    System.out.println(t2-t1);
    System.out.println(counter);
  }

}
