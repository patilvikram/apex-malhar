package org.apache.apex.malhar.python.runtime;

import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.window.Accumulation;
import org.apache.apex.malhar.lib.window.TriggerOption;
import org.apache.apex.malhar.lib.window.WindowOption;
import org.apache.apex.malhar.lib.window.WindowState;
import org.apache.apex.malhar.lib.window.impl.InMemoryWindowedKeyedStorage;
import org.apache.apex.malhar.lib.window.impl.InMemoryWindowedStorage;
import org.apache.apex.malhar.lib.window.impl.KeyedWindowedOperatorImpl;
import org.apache.apex.malhar.lib.window.impl.WindowedOperatorImpl;
import org.apache.apex.malhar.python.operator.transform.PythonFilterOperator;
import org.apache.apex.malhar.python.operator.transform.PythonFlatMapOperator;
import org.apache.apex.malhar.python.operator.PythonGenericOperator;
import org.apache.apex.malhar.python.operator.PythonKeyedWindowedOperator;
import org.apache.apex.malhar.python.operator.transform.PythonMapOperator;
import org.apache.apex.malhar.python.operator.PythonWindowedOperator;
import org.apache.apex.malhar.python.operator.proxy.PythonWorkerProxy;
import org.apache.apex.malhar.stream.api.ApexStream;
import org.apache.apex.malhar.stream.api.Option;
import org.apache.apex.malhar.stream.api.PythonApexStream;
import org.apache.apex.malhar.stream.api.WindowedStream;
import org.apache.apex.malhar.stream.api.impl.ApexStreamImpl;
import org.apache.apex.malhar.stream.api.impl.ApexWindowedStreamImpl;
import org.apache.apex.malhar.stream.api.impl.DagMeta;
import org.apache.hadoop.classification.InterfaceStability;

import com.datatorrent.api.Operator;

/**
 * Created by vikram on 7/6/17.
 */

@InterfaceStability.Evolving
public class PythonApexStreamImpl<T> extends ApexWindowedStreamImpl<T> implements PythonApexStream<T>
{
  private static final Logger LOG = LoggerFactory.getLogger(PythonApexStreamImpl.class);


  public PythonApexStreamImpl()
  {
    super();
  }

  public PythonApexStreamImpl(ApexStreamImpl<T> apexStream)
  {
    super();
    this.lastBrick = apexStream.getLastBrick();
    this.graph = apexStream.getGraph();

  }

  @Override
  public PythonApexStream<T> map(byte[] serializedFunction, Option... opts)
  {
    LOG.debug("Adding Python map operator");
    PythonGenericOperator<T> operator = new PythonMapOperator<T>(serializedFunction);
    return addOperator(operator, (Operator.InputPort<T>)operator.in, (Operator.OutputPort<T>)operator.out, opts);

  }

  @Override
  public PythonApexStream<T> flatMap(byte[] serializedFunction, Option... opts)
  {
    LOG.debug("Adding Python flatmap operator");
    PythonGenericOperator<T> operator = new PythonFlatMapOperator<T>(serializedFunction);
    return addOperator(operator, (Operator.InputPort<T>)operator.in, (Operator.OutputPort<T>)operator.out, opts);

  }

  @Override
  public PythonApexStream<T> filter(byte[] serializedFunction, Option... opts)
  {
    LOG.debug("Adding Python filter operator");
    PythonFilterOperator<T> operator = new PythonFilterOperator<>(serializedFunction);
    return addOperator(operator, (Operator.InputPort<T>)operator.in, (Operator.OutputPort<T>)operator.out, opts);

  }


//  @Override
//  public <O, STREAM extends ApexStream<O>> STREAM map(Function.MapFunction<T, O> mf, Option... opts)
//  {
//    FunctionOperator.MapFunctionOperator<T, O> opt = new FunctionOperator.MapFunctionOperator<>(mf);
//    return addOperator(opt, opt.input, opt.output, opts);
//  }

  @Override
  protected <O> ApexStream<O> newStream(DagMeta graph, Brick<O> newBrick)
  {
    PythonApexStreamImpl<O> newstream = new PythonApexStreamImpl<>();
    newstream.graph = graph;
    newstream.lastBrick = newBrick;
    newstream.windowOption = this.windowOption;
    newstream.triggerOption = this.triggerOption;
    newstream.allowedLateness = this.allowedLateness;
    return newstream;
  }


  @Override
  public WindowedStream<T> window(WindowOption windowOption, TriggerOption triggerOption, Duration allowLateness)
  {
    PythonApexStreamImpl<T> windowedStream = new PythonApexStreamImpl<>();
    windowedStream.lastBrick = lastBrick;
    windowedStream.graph = graph;
    windowedStream.windowOption = windowOption;
    windowedStream.triggerOption = triggerOption;
    windowedStream.allowedLateness = allowLateness;
    return windowedStream;
  }


  /**
   * Create the windowed operator for windowed transformation
   * @param accumulationFn
   * @param <IN>
   * @param <ACCU>
   * @param <OUT>
   * @return
   */
//  @Override
  protected <IN, ACCU, OUT> WindowedOperatorImpl<IN, ACCU, OUT> createWindowedOperator(Accumulation<? super IN, ACCU, OUT> accumulationFn)
  {

    PythonWindowedOperator windowedOperator = new PythonWindowedOperator(((PythonWorkerProxy)accumulationFn).getSerializedData());
    //TODO use other default setting in the future
    windowedOperator.setDataStorage(new InMemoryWindowedStorage<ACCU>());
    windowedOperator.setRetractionStorage(new InMemoryWindowedStorage<OUT>());
    windowedOperator.setWindowStateStorage(new InMemoryWindowedStorage<WindowState>());
    if (windowOption != null) {
      windowedOperator.setWindowOption(windowOption);
    }
    if (triggerOption != null) {
      windowedOperator.setTriggerOption(triggerOption);
    }
    if (allowedLateness != null) {
      windowedOperator.setAllowedLateness(allowedLateness);
    }
    windowedOperator.setAccumulation(accumulationFn);
    return windowedOperator;
  }

  protected <K, V, ACCU, OUT> KeyedWindowedOperatorImpl<K, V, ACCU, OUT> createKeyedWindowedOperator(Accumulation<? super V, ACCU, OUT> accumulationFn)
  {
    KeyedWindowedOperatorImpl<K, V, ACCU, OUT> keyedWindowedOperator = new PythonKeyedWindowedOperator(((PythonWorkerProxy)accumulationFn).getSerializedData());

    //TODO use other default setting in the future
    keyedWindowedOperator.setDataStorage(new InMemoryWindowedKeyedStorage<K, ACCU>());
    keyedWindowedOperator.setRetractionStorage(new InMemoryWindowedKeyedStorage<K, OUT>());
    keyedWindowedOperator.setWindowStateStorage(new InMemoryWindowedStorage<WindowState>());
    if (windowOption != null) {
      keyedWindowedOperator.setWindowOption(windowOption);
    }
    if (triggerOption != null) {
      keyedWindowedOperator.setTriggerOption(triggerOption);
    }
    if (allowedLateness != null) {
      keyedWindowedOperator.setAllowedLateness(allowedLateness);
    }

    keyedWindowedOperator.setAccumulation(accumulationFn);
    return keyedWindowedOperator;
  }


}
