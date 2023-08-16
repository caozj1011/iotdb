package org.apache.iotdb.write;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/** 阿里规范的线程池工具类示例 */
public class AliThreadPoolUtil {

  private static final int CORE_POOL_SIZE = Runtime.getRuntime().availableProcessors() * 2;
  private static final int MAX_POOL_SIZE = 20;
  private static final int QUEUE_CAPACITY = 100;
  private static final long KEEP_ALIVE_TIME = 0L;

  private static final ThreadPoolExecutor THREAD_POOL =
      new ThreadPoolExecutor(
          CORE_POOL_SIZE,
          MAX_POOL_SIZE,
          KEEP_ALIVE_TIME,
          TimeUnit.SECONDS,
          new LinkedBlockingQueue<>(QUEUE_CAPACITY),
          new ThreadPoolRejectedExecutionHandler());

  public static void execute(Runnable task) {
    THREAD_POOL.execute(task);
  }

  private static class ThreadFactoryBuilder implements ThreadFactory {
    private final ThreadFactory defaultFactory = Executors.defaultThreadFactory();
    private final ThreadGroup threadGroup;
    private final AtomicInteger threadNumber = new AtomicInteger(1);
    private final String namePrefix;

    ThreadFactoryBuilder() {
      SecurityManager s = System.getSecurityManager();
      threadGroup = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
      namePrefix = "AliThreadPoolUtil-pool-" + threadGroup.getName() + "-";
    }

    @Override
    public Thread newThread(Runnable r) {
      Thread t = defaultFactory.newThread(r);
      t.setName(namePrefix + threadNumber.getAndIncrement());
      if (t.isDaemon()) {
        t.setDaemon(false);
      }
      if (t.getPriority() != Thread.NORM_PRIORITY) {
        t.setPriority(Thread.NORM_PRIORITY);
      }
      return t;
    }
  }

  private static class ThreadPoolRejectedExecutionHandler implements RejectedExecutionHandler {
    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
      // 使用日志记录或其他处理方式来处理任务被拒绝的情况
      System.err.println("Task rejected: " + r.toString());
    }
  }
}
