import java.util.Vector;

public class ArrayTest {

  public static void main(String[] args) {
    Vector<byte[]> v = new Vector<byte[]>();
    byte[] data = new byte[3];
    data[0] = 1;
    data[1] = 2;
    data[2] = 3;
    v.addElement(data);
    data[0] = 42;
    v.add(data);
    System.out.println(v.get(0)[0]);
  }
}
