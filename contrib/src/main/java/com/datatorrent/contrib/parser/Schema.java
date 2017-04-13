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

package com.datatorrent.contrib.parser;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * <p>
 * This is schema that defines fields and their constraints for delimited and fixedWidth files
 * The operators use this information to validate the incoming tuples.
 * Information from JSON schema is saved in this object and is used by the
 * operators
 *
 * @since 3.7.0
 */
public class Schema
{
  /**
   * JSON key string for fields array
   */
  public static final String FIELDS = "fields";
  /**
   * JSON key string for name of the field within fields array
   */
  public static final String NAME = "name";
  /**
   * JSON key string for type of the field within fields array
   */
  public static final String TYPE = "type";
  /**
   * JSON key string for date format constraint
   */
  public static final String DATE_FORMAT = "format";
  /**
   * JSON key string for true value constraint
   */
  public static final String TRUE_VALUE = "trueValue";
  /**
   * JSON key string for false value constraint
   */
  public static final String FALSE_VALUE = "falseValue";
  /**
   * JSON key string for default true value of boolean
   */
  public static final String DEFAULT_TRUE_VALUE = "true";
  /**
   * JSON key string for default true value of boolean
   */
  public static final String DEFAULT_FALSE_VALUE = "false";
  /**
   * Default date format
   */
  public static final String DEFAULT_DATE_FORMAT = "dd/mm/yy";
  /**
   * This holds the list of field names in the same order as in the schema
   */
  protected List<String> fieldNames = new LinkedList<String>();


  /**
   * Get the list of field names mentioned in schema
   *
   * @return fieldNames
   */
  public List<String> getFieldNames()
  {
    return Collections.unmodifiableList(fieldNames);
  }

  /**
   * Supported data types
   */
  public enum FieldType
  {
    BOOLEAN, DOUBLE, INTEGER, FLOAT, LONG, SHORT, CHARACTER, STRING, DATE
  }

  /**
   * Objects of this class represents a particular field in the schema. Each
   * field has a name, type and a set of associated constraints.
   *
   */
  public class Field
  {
    /**
     * name of the field
     */
    String name;
    /**
     * Data type of the field
     */
    FieldType type;

    /**
     * Parameterized Constructor
     * @param name name of the field.
     * @param type data type of the field.
     */
    public Field(String name, String type)
    {
      this.name = name;
      this.type = FieldType.valueOf(type.toUpperCase());
    }

    /**
     * Default Constructor
     */
    public Field()
    {
    }

    /**
     * Get the name of the field
     *
     * @return name
     */
    public String getName()
    {
      return name;
    }

    /**
     * Set the name of the field
     *
     * @param name
     */
    public void setName(String name)
    {
      this.name = name;
    }

    /**
     * Get {@link FieldType}
     *
     * @return type
     */
    public FieldType getType()
    {
      return type;
    }

    /**
     * Set {@link FieldType}
     *
     * @param type
     */
    public void setType(FieldType type)
    {
      this.type = type;
    }

    @Override
    public String toString()
    {
      return "Fields [name=" + name + ", type=" + type + "]";
    }

  }
}
