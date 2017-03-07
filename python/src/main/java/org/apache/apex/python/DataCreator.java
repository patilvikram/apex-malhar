package org.apache.apex.python;

import java.util.Random;

/**
 * Created by vikram on 2/3/17.
 */
public class DataCreator implements Runnable
{
  private JavaWorker worker = null;

  public DataCreator(JavaWorker worker)
  {
    this.worker = worker;
  }

  @Override
  public void run()
  {
    while (!worker.isRegistered()) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

    }
    int count = 5;
    while (worker.isRegistered() && count > 0) {

      try {
        Random random = new Random();
        int a = random.nextInt();
        int b = random.nextInt();
        Thread.sleep(5000);
        count--;
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

    }
    worker.shutdown();

  }
}
