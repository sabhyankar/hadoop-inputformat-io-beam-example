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

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.io.Text;

import java.util.List;

public final class SchemaConstants {

  public static final String AVRO_JSON_SCHEMA_FULL =
      "{\n"
          + "  \"type\" : \"record\",\n"
          + "  \"name\" : \"topLevelRecord\",\n"
          + "  \"fields\" : [ {\n"
          + "    \"name\" : \"name\",\n"
          + "    \"type\" : [ \"string\", \"null\" ]\n"
          + "  }, {\n"
          + "    \"name\" : \"age\",\n"
          + "    \"type\" : [ \"int\", \"null\" ]\n"
          + "  }, {\n"
          + "    \"name\" : \"addresses\",\n"
          + "    \"type\" : [ {\n"
          + "      \"type\" : \"array\",\n"
          + "      \"items\" : [ {\n"
          + "        \"type\" : \"record\",\n"
          + "        \"name\" : \"address\",\n"
          + "        \"namespace\" : \"addresses\",\n"
          + "        \"fields\" : [ {\n"
          + "          \"name\" : \"type\",\n"
          + "          \"type\" : [ \"string\", \"null\" ]\n"
          + "        }, {\n"
          + "          \"name\" : \"address_line\",\n"
          + "          \"type\" : [ \"string\", \"null\" ]\n"
          + "        }, {\n"
          + "          \"name\" : \"city\",\n"
          + "          \"type\" : [ \"string\", \"null\" ]\n"
          + "        }, {\n"
          + "          \"name\" : \"country\",\n"
          + "          \"type\" : [ \"string\", \"null\" ]\n"
          + "        }, {\n"
          + "          \"name\" : \"state\",\n"
          + "          \"type\" : [ \"string\", \"null\" ]\n"
          + "        }, {\n"
          + "          \"name\" : \"zip\",\n"
          + "          \"type\" : [ \"string\", \"null\" ]\n"
          + "        } ]\n"
          + "      }, \"null\" ]\n"
          + "    }, \"null\" ]\n"
          + "  }, {\n"
          + "    \"name\" : \"emails\",\n"
          + "    \"type\" : [ {\n"
          + "      \"type\" : \"array\",\n"
          + "      \"items\" : [ \"string\", \"null\" ]\n"
          + "    }, \"null\" ]\n"
          + "  } ]\n"
          + "}";

  public static final String AVRO_JSON_SCHEMA_ADDRESS =
      "{\n"
          + "  \"type\": \"record\",\n"
          + "  \"name\": \"address\",\n"
          + "  \"namespace\": \"addresses\",\n"
          + "  \"fields\": [\n"
          + "    {\n"
          + "      \"name\": \"type\",\n"
          + "      \"type\": [\"string\", \"null\"]\n"
          + "    },\n"
          + "    {\n"
          + "      \"name\": \"address_line\",\n"
          + "      \"type\": [\"string\", \"null\"]\n"
          + "    },\n"
          + "    {\n"
          + "      \"name\": \"city\",\n"
          + "      \"type\": [\"string\", \"null\"]\n"
          + "    },\n"
          + "    {\n"
          + "      \"name\": \"country\",\n"
          + "      \"type\": [\"string\", \"null\"]\n"
          + "    },\n"
          + "    {\n"
          + "      \"name\": \"state\",\n"
          + "      \"type\": [\"string\", \"null\"]\n"
          + "    },\n"
          + "    {\n"
          + "      \"name\": \"zip\",\n"
          + "      \"type\": [\"string\", \"null\"]\n"
          + "    }\n"
          + "  ]\n"
          + "}";

  public static final String NAME = "John Smith";
  public static final Integer AGE = 30;
  public static final List<Text> EMAIL_ADDRESSES =
      ImmutableList.of(new Text("abc@xyz.com"), new Text("def@ghi.com"));
  public static final String ADDRESS_1_TYPE = "Home";
  public static final String ADDRESS_2_TYPE = "Office";
  public static final String ADDRESS_LINE = "123 abc street";
  public static final String CITY = "Schenectady";
  public static final String STATE = "NY";
  public static final String COUNTRY = "USA";
  public static final String ZIP = "12345";

  private SchemaConstants() {}
}
