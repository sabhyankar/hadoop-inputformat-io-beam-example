/*
 * Copyright 2018 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.pso.transformation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;

import org.apache.hadoop.io.*;
import org.apache.orc.mapred.OrcList;
import org.apache.orc.mapred.OrcMap;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapred.OrcTimestamp;
import org.apache.orc.mapred.OrcUnion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A helper class {@link OrcStructToAvroFn} that converts a OrcStruct {@link OrcStruct} to a
 * GenericRecord {@link GenericRecord}
 */
public class OrcStructToAvroFn extends SimpleFunction<OrcStruct, GenericRecord> {

  private String schemaString;
  private static final Logger LOG = LoggerFactory.getLogger(OrcStructToAvroFn.class);

  public OrcStructToAvroFn(String schemaString) {
    this.schemaString = schemaString;
  }

  @Override
  public GenericRecord apply(OrcStruct input) {
    return getAvroRecord(input, this.schemaString);
  }

  /**
   * Since Schema {@link Schema} objects are not serializable, this helper method will accept a
   * String schema and convert it to an Avro schema
   *
   * @param struct An OrcStruct object
   * @param schString Avro Schema as a String
   * @return GenericRecord An Avro GenericRecord object
   */
  private GenericRecord getAvroRecord(OrcStruct struct, String schString) {
    Schema schema = (new Schema.Parser()).parse(schString);
    return getAvroRecord(struct, schema);
  }

  private GenericRecord getAvroRecord(OrcStruct struct, Schema schema) {
    GenericRecord genericRecord = new GenericData.Record(schema);

    for (Schema.Field field : schema.getFields()) {
      WritableComparable writableComparable = struct.getFieldValue(field.name());
      Schema baseSchema = getBaseType(field.schema());

      Object baseObject = getBaseObject(writableComparable, baseSchema);
      genericRecord.put(field.name(), baseObject);
    }
    return genericRecord;
  }

  /**
   * A utility method to convert an Orc WritableComparable object to a generic Java object that can
   * be added to an Avro GenericRecord
   *
   * @param w Orc WritableComparable to convert
   * @param fieldSchema Avro Schema
   * @return Object that will be added to the Avro GenericRecord
   */
  private Object getBaseObject(WritableComparable w, Schema fieldSchema) {
    Object obj = null;

    if (w == null || NullWritable.class.isAssignableFrom(w.getClass())) {
      obj = null;
    } else if (BooleanWritable.class.isAssignableFrom(w.getClass())) {
      obj = ((BooleanWritable) w).get();
    } else if (ByteWritable.class.isAssignableFrom(w.getClass())) {
      obj = ((ByteWritable) w).get();
    } else if (ShortWritable.class.isAssignableFrom(w.getClass())) {
      obj = ((ShortWritable) w).get();
    } else if (IntWritable.class.isAssignableFrom(w.getClass())) {
      obj = ((IntWritable) w).get();
    } else if (LongWritable.class.isAssignableFrom(w.getClass())) {
      obj = ((LongWritable) w).get();
    } else if (FloatWritable.class.isAssignableFrom(w.getClass())) {
      obj = ((FloatWritable) w).get();
    } else if (DoubleWritable.class.isAssignableFrom(w.getClass())) {
      obj = ((DoubleWritable) w).get();
    } else if (BytesWritable.class.isAssignableFrom(w.getClass())) {
      obj = ((BytesWritable) w).getBytes();
    } else if (Text.class.isAssignableFrom(w.getClass())) {
      obj = ((Text) w).toString();
    } else if (DateWritable.class.isAssignableFrom(w.getClass())) {
      obj = ((DateWritable) w).get();
    } else if (OrcTimestamp.class.isAssignableFrom(w.getClass())) {
      obj = ((OrcTimestamp) w).getTime();
    } else if (HiveDecimalWritable.class.isAssignableFrom(w.getClass())) {
      obj = ((HiveDecimalWritable) w).doubleValue();
    } else if (OrcStruct.class.isAssignableFrom(w.getClass())) {
      obj = getAvroRecord((OrcStruct) w, fieldSchema);
    } else if (OrcList.class.isAssignableFrom(w.getClass())) {
      obj = translateList((OrcList) w, fieldSchema);
    } else if (OrcMap.class.isAssignableFrom(w.getClass())) {
      obj = translateMap((OrcMap) w, fieldSchema);
    } else if (OrcUnion.class.isAssignableFrom(w.getClass())) {
      // TODO - Verify if this is even valid
      LOG.info("Skipping unimplemented type: union");
    } else {
      LOG.info("Unknown type found: " + w.getClass().getSimpleName());
      throw new IllegalArgumentException("Unknown type: " + w.getClass().getSimpleName());
    }

    return obj;
  }

  private List<Object> translateList(OrcList<? extends WritableComparable> l, Schema fieldSchema) {
    if (l == null || l.size() < 1) {
      return ImmutableList.of();
    }

    Schema elementTypeUnion = getBaseType(fieldSchema).getElementType();

    List<Object> retArray = new ArrayList<>(l.size());

    for (WritableComparable w : l) {
      Object o = getBaseObject(w, getBaseType(elementTypeUnion));
      retArray.add(o);
    }
    return ImmutableList.copyOf(retArray);
  }

  private Map<String, Object> translateMap(
      OrcMap<WritableComparable, WritableComparable> m, Schema fieldSchema) {
    if (m == null || m.size() < 1) {
      return ImmutableMap.of();
    }

    Schema valueType = getBaseType(fieldSchema).getValueType();
    Schema keyType = Schema.create(Schema.Type.STRING);

    Map<String, Object> ret = new HashMap<>();
    for (Map.Entry<WritableComparable, WritableComparable> entry : m.entrySet()) {
      Object mappedValue = getBaseObject(entry.getValue(), valueType);
      String key = getBaseObject(entry.getKey(), keyType).toString();
      ret.put(key, mappedValue);
    }

    return ImmutableMap.copyOf(ret);
  }

  private Schema getBaseType(Schema currSchema) {
    if (currSchema == null || !Schema.Type.UNION.equals(currSchema.getType())) {
      return currSchema;
    }

    Optional<Schema> optionalSchema =
        currSchema
            .getTypes()
            .stream()
            .filter(s -> !Schema.Type.NULL.equals(s.getType()))
            .findFirst();

    if (!optionalSchema.isPresent()) {
      throw new IllegalArgumentException("Need exactly one non-null type in a UNION");
    }

    return optionalSchema.get();
  }
}
