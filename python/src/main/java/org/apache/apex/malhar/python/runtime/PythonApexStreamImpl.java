package org.apache.apex.malhar.python.runtime;

import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.window.TriggerOption;
import org.apache.apex.malhar.lib.window.WindowOption;
import org.apache.apex.malhar.python.operator.PythonFilterOperator;
import org.apache.apex.malhar.python.operator.PythonFlatMapOperator;
import org.apache.apex.malhar.python.operator.PythonGenericOperator;
import org.apache.apex.malhar.python.operator.PythonMapOperator;
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
//  public PythonApexStreamImpl(PythonApexStreamImpl apexStream)
//  {
//      super(apexStream);
//  }


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





}
