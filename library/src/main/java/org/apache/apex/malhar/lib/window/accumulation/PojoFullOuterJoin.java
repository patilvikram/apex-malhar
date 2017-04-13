/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.lib.window.accumulation;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceStability;

import com.google.common.base.Throwables;
import com.google.common.collect.Multimap;

import com.datatorrent.lib.util.KeyValPair;
import com.datatorrent.lib.util.PojoUtils;

/**
 * Full outer join Accumulation for Pojo Streams.
 *
 *
 * @since 3.7.0
 */
@InterfaceStability.Evolving
public class PojoFullOuterJoin<InputT1, InputT2>
    extends AbstractPojoJoin<InputT1, InputT2>
{
  public PojoFullOuterJoin()
  {
   super();
  }

  public PojoFullOuterJoin(Class<?> outClass, String[] leftKeys, String[] rightKeys)
  {
    super(outClass,leftKeys,rightKeys);
  }

  public PojoFullOuterJoin(Class<?> outClass, String[] leftKeys, String[] rightKeys, Map<String, KeyValPair<STREAM, String>> outputToInputMap)
  {
    super(outClass,leftKeys,rightKeys, outputToInputMap);
  }

  @Override
  public void addNonMatchingResult(Collection<Object> left, Map<String,PojoUtils.Getter> leftGettersStream, List<Object> result)
  {
    for (Object lObj:left) {
      Object o;
      try {
        o = outClass.newInstance();
      } catch (Throwable e) {
        throw Throwables.propagate(e);
      }
      if (outputToInputMap != null) {
        for (Map.Entry<String, KeyValPair<STREAM,String>> entry : outputToInputMap.entrySet()) {
          if (entry.getValue().getKey() == STREAM.LEFT) {
            setters.get(entry.getKey()).set(o, leftGettersStream.get(entry.getValue().getValue()).get(lObj));
          }
        }
      } else {
        setObjectForResult(leftGettersStream, lObj, o);
      }
      result.add(o);
    }
  }

  @Override
  public void addNonMatchingRightStream(Multimap<List<Object>, Object> rightStream,
      Map<String,PojoUtils.Getter> rightGettersStream, List<Object> result)
  {
    for (Object key : rightStream.keySet()) {
      if (outputToInputMap == null) {
        addNonMatchingResult(rightStream.get((List)key), rightGettersStream, result);
      } else {
        for (Object obj:  rightStream.get((List)key)) {
          Object o;
          try {
            o = outClass.newInstance();
          } catch (Throwable e) {
            throw Throwables.propagate(e);
          }
          for (Map.Entry<String, KeyValPair<STREAM,String>> entry : outputToInputMap.entrySet()) {
            if (entry.getValue().getKey() == STREAM.RIGHT) {
              setters.get(entry.getKey()).set(o, rightGettersStream.get(entry.getValue().getValue()).get(obj));
            }
          }
          result.add(o);
        }
      }
    }
  }

  @Override
  public int getLeftStreamIndex()
  {
    return 0;
  }
}
