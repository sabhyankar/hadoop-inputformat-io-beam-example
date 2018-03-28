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
package com.google.cloud.pso;

import static org.junit.Assert.assertEquals;

import com.google.cloud.pso.transformation.OrcStructToAvroFn;
import com.google.common.collect.ImmutableList;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcList;
import org.apache.orc.mapred.OrcStruct;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;
import java.util.stream.Collectors;

/** Test class for {@link HadoopInputFormatIOExample} */
@RunWith(JUnit4.class)
public class HadoopInputFormatIOExampleTest {

  private static OrcStruct struct;
  private static Schema schema;
  private static GenericRecord genericRecord;

  @Before
  public void setUp() {
    setUpOrcStruct();
    setUpAvroGenericRecord();
  }

  private void setUpOrcStruct() {
    /* Define the orc schema - TypeDescription */
    TypeDescription orcTypeDesc = TypeDescription.createStruct();
    orcTypeDesc.addField("name", TypeDescription.createString());
    orcTypeDesc.addField("age", TypeDescription.createInt());

    TypeDescription address = TypeDescription.createStruct();
    address.addField("type", TypeDescription.createString());
    address.addField("address_line", TypeDescription.createString());
    address.addField("city", TypeDescription.createString());
    address.addField("country", TypeDescription.createString());
    address.addField("state", TypeDescription.createString());
    address.addField("zip", TypeDescription.createString());
    orcTypeDesc.addField("addresses", TypeDescription.createList(address));
    orcTypeDesc.addField("emails", TypeDescription.createList(TypeDescription.createString()));

    OrcStruct address_1 = new OrcStruct(address);
    address_1.setFieldValue("type", new Text(SchemaConstants.ADDRESS_1_TYPE));
    address_1.setFieldValue("address_line", new Text(SchemaConstants.ADDRESS_LINE));
    address_1.setFieldValue("city", new Text(SchemaConstants.CITY));
    address_1.setFieldValue("country", new Text(SchemaConstants.COUNTRY));
    address_1.setFieldValue("state", new Text(SchemaConstants.STATE));
    address_1.setFieldValue("zip", new Text(SchemaConstants.ZIP));

    OrcStruct address_2 = new OrcStruct(address);
    address_2.setFieldValue("type", new Text(SchemaConstants.ADDRESS_2_TYPE));
    address_2.setFieldValue("address_line", new Text(SchemaConstants.ADDRESS_LINE));
    address_2.setFieldValue("city", new Text(SchemaConstants.CITY));
    address_2.setFieldValue("country", new Text(SchemaConstants.COUNTRY));
    address_2.setFieldValue("state", new Text(SchemaConstants.STATE));
    address_2.setFieldValue("zip", new Text(SchemaConstants.ZIP));

    TypeDescription typeAddresses = TypeDescription.createList(address);
    OrcList<OrcStruct> addresses = new OrcList<>(typeAddresses);
    addresses.add(address_1);
    addresses.add(address_2);

    TypeDescription typeEmails = TypeDescription.createList(TypeDescription.createString());
    OrcList<Text> emails = new OrcList<>(typeEmails);
    emails.addAll(SchemaConstants.EMAIL_ADDRESSES);

    struct = new OrcStruct(orcTypeDesc);
    struct.setFieldValue("name", new Text(SchemaConstants.NAME));
    struct.setFieldValue("age", new IntWritable(SchemaConstants.AGE));
    struct.setFieldValue("addresses", addresses);
    struct.setFieldValue("emails", emails);
  }

  private void setUpAvroGenericRecord() {

    schema = (new Schema.Parser()).parse(SchemaConstants.AVRO_JSON_SCHEMA_FULL);
    genericRecord = new GenericData.Record(schema);
    genericRecord.put("name", SchemaConstants.NAME);
    genericRecord.put("age", SchemaConstants.AGE);

    Schema addressSchema = (new Schema.Parser()).parse(SchemaConstants.AVRO_JSON_SCHEMA_ADDRESS);
    GenericRecord address_1 = new GenericData.Record(addressSchema);
    GenericRecord address_2 = new GenericData.Record(addressSchema);
    address_1.put("type", SchemaConstants.ADDRESS_1_TYPE);
    address_1.put("address_line", SchemaConstants.ADDRESS_LINE);
    address_1.put("city", SchemaConstants.CITY);
    address_1.put("country", SchemaConstants.COUNTRY);
    address_1.put("state", SchemaConstants.STATE);
    address_1.put("zip", SchemaConstants.ZIP);

    address_2.put("type", SchemaConstants.ADDRESS_2_TYPE);
    address_2.put("address_line", SchemaConstants.ADDRESS_LINE);
    address_2.put("city", SchemaConstants.CITY);
    address_2.put("country", SchemaConstants.COUNTRY);
    address_2.put("state", SchemaConstants.STATE);
    address_2.put("zip", SchemaConstants.ZIP);

    genericRecord.put("addresses", ImmutableList.of(address_1, address_2));

    List<String> listOfEmails =
        SchemaConstants.EMAIL_ADDRESSES
            .stream()
            .map(e -> e.toString())
            .collect(Collectors.toList());
    genericRecord.put("emails", listOfEmails);
  }

  /** Test whether the SimpleFunction has the right type descriptor */
  @Test
  public void testTypeDescriptor() {
    SimpleFunction<OrcStruct, GenericRecord> fn = new OrcStructToAvroFn(schema.toString());
    assertEquals(fn.getInputTypeDescriptor(), TypeDescriptor.of(OrcStruct.class));
    assertEquals(fn.getOutputTypeDescriptor(), TypeDescriptor.of(GenericRecord.class));
  }

  /** Test whether an OrcStruct object gets converted correctly into a GenericRecord */
  @Test
  public void testOrcStructToAvroFn() {
    SimpleFunction<OrcStruct, GenericRecord> fn = new OrcStructToAvroFn(schema.toString());
    assertEquals(genericRecord, fn.apply(struct));
  }
}
