/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro.io;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.junit.Test;

public class TestJsonDecoderExtraField {

  @Test
  public void testInt() throws Exception {
    checkExtraRecordField("int", 1);
  }


  private void checkExtraRecordField(String type, Object value) throws Exception {
    String def =
            "{\"type\":\"record\",\"name\":\"X\",\"fields\":" +
                    "[{\"type\":\"" + type + "\",\"name\":\"n\"}]}";
    Schema schema = Schema.parse(def);
    DatumReader<GenericRecord> reader =
            new GenericDatumReader<>(schema);

    String[] records = {"{\"n\":1,\"name\":\"avro\"}", "{\"n\":1.0}"};

    for (String record : records) {
      Decoder decoder = DecoderFactory.get().jsonDecoder(schema, record);
      GenericRecord r = reader.read(null, decoder);
      Assert.assertEquals(value, r.get("n"));
    }

  }

  // array of records
  // it works.
  @Test
  public void testArrayofRecords() throws Exception {

    String w = "{\"type\":\"record\",\"name\":\"R\",\"fields\":" +
            "[ {\"type\":\"long\",\"name\":\"l\"}," +
            "{\"name\":\"items\",\"type\":" +
            "{\"type\":\"array\",\"items\":" +
            "{\"type\":\"record\",\"name\":\"X\",\"fields\":" +
            "[ {\"name\":\"project\",\"type\":\"string\"},{\"name\":\"lib\",\"type\":\"string\"}]}}}]}";

    Schema ws = Schema.parse(w);
    DecoderFactory df = DecoderFactory.get();
    String data = "{\"l\":100,\"items\":[{\"project\": \"avro\",\"lib\": \"avrojson\"}]}";
    JsonDecoder in = df.jsonDecoder(ws, data);
    Assert.assertEquals(100, in.readLong());
  }

  // array of records
  // it works
  @Test
  public void testArrayExtraField() throws Exception {

    String w = "{\"type\":\"record\",\"name\":\"R\",\"fields\":" +
            "[ {\"type\":\"long\",\"name\":\"l\"}," +
            "{\"name\":\"items\",\"type\":" +
            "{\"type\":\"array\",\"items\":" +
            "{\"type\":\"record\",\"name\":\"X\",\"fields\":" +
            "[ {\"name\":\"project\",\"type\":\"string\"},{\"name\":\"lib\",\"type\":\"string\"}]}}}]}";

    Schema ws = Schema.parse(w);
    DecoderFactory df = DecoderFactory.get();
    String data = "{\"l\":100,\"items\":[{\"project\": \"avro\",\"lib\": \"avrojson\",\"extraField\":\"test\"}]}";
    JsonDecoder in = df.jsonDecoder(ws, data);
    Assert.assertEquals(100, in.readLong());
  }
}
