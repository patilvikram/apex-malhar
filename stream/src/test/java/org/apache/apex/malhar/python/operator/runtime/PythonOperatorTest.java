package org.apache.apex.malhar.python.operator.runtime;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.python.operator.PythonMapOperator;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.LocalMode;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.stram.StramLocalCluster;

public class PythonOperatorTest
{
  private static int TupleCount;
  private static List<String> lengthList = new ArrayList<>();
  private static final int NumTuples = 10;

  public static class NumberGenerator extends BaseOperator implements InputOperator
  {
    private int num;

    public final transient DefaultOutputPort<String> output = new DefaultOutputPort<String>();

    @Override
    public void setup(Context.OperatorContext context)
    {
      num = 0;
    }

    @Override
    public void emitTuples()
    {
      if (num < NumTuples) {
        output.emit(Integer.toString(num));
        num++;
      }
    }
  }

  public static class PythonMapWorker implements PythonWorker
  {

    @Override
    public Object setFunction(byte[] func, String opType)
    {
      return opType;
    }

    @Override
    public Object execute(Object tuple)
    {

      return tuple;
    }
  }

  public static class ResultCollector extends BaseOperator
  {

    public final transient DefaultInputPort<String> input = new DefaultInputPort<String>()
    {

      @Override
      public void process(String in)
      {
        TupleCount++;
        lengthList.add(in);
      }
    };

    @Override
    public void setup(Context.OperatorContext context)
    {
      TupleCount = 0;
      lengthList = new ArrayList<>();
    }

  }

  @Test
  public void testPythonMapOperator()
  {

    LocalMode lma = LocalMode.newInstance();
    DAG dag = lma.getDAG();

    NumberGenerator numGen = dag.addOperator("numGen", new NumberGenerator());

    ResultCollector collector = dag.addOperator("collector", new ResultCollector());
    PythonMapOperator<String> mapOperator = new PythonMapOperator<>();
    dag.addOperator("mapOperator", mapOperator);

    dag.addStream("raw numbers", numGen.output, mapOperator.in);
    dag.addStream("mapped results", mapOperator.out, collector.input);

    // Create local cluster
    LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(false);

    ((StramLocalCluster)lc).setExitCondition(new Callable<Boolean>()
    {
      @Override
      public Boolean call() throws Exception
      {
        return TupleCount == NumTuples;
      }
    });

    lc.run(5000);

    Assert.assertEquals(NumTuples,TupleCount );
  }
}
