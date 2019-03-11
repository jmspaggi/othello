package org.spaggiari.othello.util;

import java.lang.reflect.Field;

import sun.misc.Unsafe;

public class ByteArrayBuilder 

{
  private static Unsafe UNSAFE;
  static
  {
    try {
      Field f = Unsafe.class.getDeclaredField("theUnsafe");
      f.setAccessible(true);
      UNSAFE = (Unsafe) f.get(null);
    } catch (Exception e) {
      throw new ExceptionInInitializerError("sun.misc.Unsafe not instantiated [" + e.toString() + "]");
    }
  }
  private long _address = -1;
  private int byteSize = 0;

  /*
   * (non-Javadoc)
   * @see com.vortex.files.util.ArrayBuilder#toArray()
   */
  public byte[] toArray() {
    byte[] values = new byte[byteSize];
    UNSAFE.copyMemory(null, _address,
      values, Unsafe.ARRAY_BYTE_BASE_OFFSET,
      byteSize);
    return values;
  }

  public ByteArrayBuilder()
  {
    appendFirst(new byte[0]);
  }

  public ByteArrayBuilder(byte[] bytes)
  {
    appendFirst(bytes);
  }

  /*
   * (non-Javadoc)
   * @see com.vortex.files.util.ArrayBuilder#append(byte[])
   */
  public ByteArrayBuilder append(byte[] _bytes)
  {
    return appendNext(_bytes);
  }

  private ByteArrayBuilder appendFirst(byte[] _bytes)
  {
    long _address2 = UNSAFE.allocateMemory(byteSize + _bytes.length);
    UNSAFE.copyMemory(_bytes, Unsafe.ARRAY_BYTE_BASE_OFFSET, null, _address2, _bytes.length);
    _address = _address2;
    byteSize += _bytes.length;
    return this;
  }

  private ByteArrayBuilder appendNext(byte[] _bytes)
  {
    long _address2 = UNSAFE.allocateMemory(byteSize + _bytes.length);
    UNSAFE.copyMemory(_address, _address2, byteSize);
    UNSAFE.copyMemory(_bytes, Unsafe.ARRAY_BYTE_BASE_OFFSET + byteSize, null, _address2, _bytes.length);
    UNSAFE.freeMemory(_address);
    _address = _address2;
    byteSize += _bytes.length;
    return this;
  }

  /*
   * (non-Javadoc)
   * @see com.vortex.files.util.ArrayBuilder#free()
   */
  public void free() {
    UNSAFE.freeMemory(_address);
  }
}