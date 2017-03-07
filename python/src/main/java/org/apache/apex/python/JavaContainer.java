package org.apache.apex.python;

import py4j.GatewayServer;

/**
 * Created by vikram on 1/3/17.
 */

public class JavaContainer
{

  public static void main(String[] args)
  {

    JavaWorker worker = new JavaWorker();
    GatewayServer server = new GatewayServer(worker);
    server.start();
    worker.setGatewayServer(server);
    worker.setGatewayPort(server.getPort());

    System.out.println(" Gateway server registration " + server.getAddress() + "  " + server.getPort());
    DataCreator creator = new DataCreator(worker);

    Thread workerThread = new Thread(creator);
    workerThread.start();
    worker.spawnProcess();

    Thread workerThread2 = new Thread(worker);
    workerThread2.start();
    server.addListener(new JavaPy4jServerConnection(worker.getPythonProcess(), worker.getGatewayServer()));

  }

}
