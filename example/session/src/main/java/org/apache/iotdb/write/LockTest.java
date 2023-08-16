package org.apache.iotdb.write;

import java.util.Arrays;
import java.util.List;

public class LockTest {
  public static void main(String[] args) throws InterruptedException {
    A a = new A();
    List<String> lit = a.getLit();
    new Thread(() -> a.lit = null).start();
    Thread.sleep(1000);
    System.out.println(lit);
  }

  static class A {
    public List<String> lit = Arrays.asList("sss", "sssss");
    public StringBuilder sb = new StringBuilder();

    public List<String> getLit() {
      return lit;
    }

    public StringBuilder getSb() {
      return sb;
    }
  }
}
