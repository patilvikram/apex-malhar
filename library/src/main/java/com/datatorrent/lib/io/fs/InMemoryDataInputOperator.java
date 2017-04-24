package com.datatorrent.lib.io.fs;

import java.util.List;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;

/**
 * Created by vikram on 19/4/17.
 */
public class InMemoryDataInputOperator<T> implements InputOperator
{

  private List<T> inputData = null;
  private boolean emissionCompleted = false;
  public final transient DefaultOutputPort<T> outputPort = new DefaultOutputPort<T>();

  public InMemoryDataInputOperator()
  {
    inputData = null;
  }

  public InMemoryDataInputOperator(List<T> data)
  {
    inputData = data;
  }

  @Override
  public void emitTuples()
  {
    if (emissionCompleted) {
      return;
    }
    for (T data : inputData) {
      outputPort.emit(data);
    }
    emissionCompleted = true;
  }

  @Override
  public void beginWindow(long l)
  {

  }

  @Override
  public void endWindow()
  {

  }

  @Override
  public void setup(Context.OperatorContext context)
  {

  }

  @Override
  public void teardown()
  {

  }
}
