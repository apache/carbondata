/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.presto.integrationtest

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, File, InputStream}
import java.util

import scala.collection.JavaConverters._

import org.apache.avro
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DecoderFactory, Encoder}
import org.junit.Assert

import org.apache.carbondata.sdk.file.CarbonWriter

class GenerateFiles {

  def singleLevelArrayFile() = {
    val json1: String =
      """ {"stringCol": "bob","intCol": 14,"doubleCol": 10.5,"realCol": 12.7,
        |"boolCol": true,"arrayStringCol1":["Street1"],"arrayStringCol2": ["India", "Egypt"],
        |"arrayIntCol": [1,2,3],"arrayBigIntCol":[70000,600000000],"arrayRealCol":[1.111,2.2],
        |"arrayDoubleCol":[1.1,2.2,3.3], "arrayBooleanCol": [true, false, true]} """.stripMargin
    val json2: String =
      """ {"stringCol": "Alex","intCol": 15,"doubleCol": 11.5,"realCol": 13.7,
        |"boolCol": true, "arrayStringCol1": ["Street1", "Street2"],"arrayStringCol2": ["Japan",
        |"China", "India"],"arrayIntCol": [1,2,3,4],"arrayBigIntCol":[70000,600000000,8000],
        |"arrayRealCol":[1.1,2.2,3.3],"arrayDoubleCol":[1.1,2.2,4.45,3.3],
        |"arrayBooleanCol": [true, true, true]} """.stripMargin
    val json3: String =
      """ {"stringCol": "Rio","intCol": 16,"doubleCol": 12.5,"realCol": 14.7,
        |"boolCol": true, "arrayStringCol1": ["Street1", "Street2","Street3"],
        |"arrayStringCol2": ["China", "Brazil", "Paris", "France"],"arrayIntCol": [1,2,3,4,5],
        |"arrayBigIntCol":[70000,600000000,8000,9111111111],"arrayRealCol":[1.1,2.2,3.3,4.45],
        |"arrayDoubleCol":[1.1,2.2,4.45,5.5,3.3], "arrayBooleanCol": [true, false, true]} """
        .stripMargin
    val json4: String =
      """ {"stringCol": "bob","intCol": 14,"doubleCol": 10.5,"realCol": 12.7,
        |"boolCol": true, "arrayStringCol1":["Street1"],"arrayStringCol2": ["India", "Egypt"],
        |"arrayIntCol": [1,2,3],"arrayBigIntCol":[70000,600000000],"arrayRealCol":[1.1,2.2],
        |"arrayDoubleCol":[1.1,2.2,3.3], "arrayBooleanCol": [true, false, true]} """.stripMargin
    val json5: String =
      """ {"stringCol": "Alex","intCol": 15,"doubleCol": 11.5,"realCol": 13.7,
        |"boolCol": true, "arrayStringCol1": ["Street1", "Street2"],"arrayStringCol2": ["Japan",
        |"China", "India"],"arrayIntCol": [1,2,3,4],"arrayBigIntCol":[70000,600000000,8000],
        |"arrayRealCol":[1.1,2.2,3.3],"arrayDoubleCol":[4,1,21.222,15.231],
        |"arrayBooleanCol": [false, false, false]} """.stripMargin


    val mySchema =
      """ {
        |      "name": "address",
        |      "type": "record",
        |      "fields": [
        |      {
        |      "name": "stringCol",
        |      "type": "string"
        |      },
        |      {
        |      "name": "intCol",
        |      "type": "int"
        |      },
        |      {
        |      "name": "doubleCol",
        |      "type": "double"
        |      },
        |      {
        |      "name": "realCol",
        |      "type": "float"
        |      },
        |      {
        |      "name": "boolCol",
        |      "type": "boolean"
        |      },
        |      {
        |      "name": "arrayStringCol1",
        |      "type": {
        |      "type": "array",
        |      "items": {
        |      "name": "street",
        |      "type": "string"
        |      }
        |      }
        |      },
        |      {
        |      "name": "arrayStringCol2",
        |      "type": {
        |      "type": "array",
        |      "items": {
        |      "name": "street",
        |      "type": "string"
        |      }
        |      }
        |      },
        |      {
        |      "name": "arrayIntCol",
        |      "type": {
        |      "type": "array",
        |      "items": {
        |      "name": "street",
        |      "type": "int"
        |      }
        |      }
        |      },
        |      {
        |      "name": "arrayBigIntCol",
        |      "type": {
        |      "type": "array",
        |      "items": {
        |      "name": "street",
        |      "type": "long"
        |      }
        |      }
        |      },
        |      {
        |      "name": "arrayRealCol",
        |      "type": {
        |      "type": "array",
        |      "items": {
        |      "name": "street",
        |      "type": "float"
        |      }
        |      }
        |      },
        |      {
        |      "name": "arrayDoubleCol",
        |      "type": {
        |      "type": "array",
        |      "items": {
        |      "name": "street",
        |      "type": "double"
        |      }
        |      }
        |      },
        |      {
        |      "name": "arrayBooleanCol",
        |      "type": {
        |      "type": "array",
        |      "items": {
        |      "name": "street",
        |      "type": "boolean"
        |      }
        |      }
        |      }
        |      ]
        |  }
                   """.stripMargin

    val nn = new avro.Schema.Parser().parse(mySchema)
    val record1 = jsonToAvro(json1, mySchema)
    val record2 = jsonToAvro(json2, mySchema)
    val record3 = jsonToAvro(json3, mySchema)
    val record4 = jsonToAvro(json4, mySchema)
    val record5 = jsonToAvro(json5, mySchema)
    var writerPath = new File(this.getClass.getResource("/").getPath
                              + "../../target/store/sdk_output/files")
      .getCanonicalPath
    //getCanonicalPath gives path with \, but the code expects /.
    writerPath = writerPath.replace("\\", "/")
    try {
      val writer = CarbonWriter.builder
        .outputPath(writerPath)
        .enableLocalDictionary(false)
        .uniqueIdentifier(System.currentTimeMillis())
        .withAvroInput(nn)
        .writtenBy("GenerateFiles")
        .build()
      writer.write(record1)
      writer.write(record2)
      writer.write(record3)
      writer.write(record4)
      writer.write(record5)
      writer.close()
    } catch {
      case e: Exception =>
        Assert.fail(e.getMessage)
    }
  }

  def twoLevelArrayFile() = {
    val json1 =
      """   {
        |         "arrayArrayInt": [[1,2,3], [4,5]],
        |         "arrayArrayBigInt":[[90000,600000000],[8000],[911111111]],
        |         "arrayArrayReal":[[1.111,2.2], [9.139,2.98]],
        |         "arrayArrayDouble":[[1.111,2.2], [9.139,2.98989898]],
        |         "arrayArrayString":[["Japan", "China"], ["India"]],
        |         "arrayArrayBoolean":[[false, false], [false]]
        |        }   """.stripMargin
    val json2 =
      """   {
        |         "arrayArrayInt": [[1,2,3], [0,5], [1,2,3,4,5], [4,5]],
        |         "arrayArrayBigInt":[[40000, 600000000, 8000],[9111111111]],
        |         "arrayArrayReal":[[1.111, 2.2], [9.139, 2.98], [9.99]],
        |         "arrayArrayDouble":[[1.111, 2.2],[9.139777, 2.98],[9.99888]],
        |         "arrayArrayString":[["China", "Brazil"], ["Paris", "France"]],
        |         "arrayArrayBoolean":[[false], [true, false]]
        |        }   """.stripMargin
    val json3 =
      """   {
        |         "arrayArrayInt": [[1], [0], [3], [4,5]],
        |         "arrayArrayBigInt":[[5000],[600000000],[8000,9111111111],[20000],[600000000,
        |         8000,9111111111]],
        |         "arrayArrayReal":[[9.198]],
        |         "arrayArrayDouble":[[0.1987979]],
        |         "arrayArrayString":[["Japan", "China", "India"]],
        |         "arrayArrayBoolean":[[false, true, false]]
        |        }   """.stripMargin
    val json4 =
      """   {
        |         "arrayArrayInt": [[0,9,0,1,3,2,3,4,7]],
        |         "arrayArrayBigInt":[[5000, 600087000, 8000, 9111111111, 20000, 600000000, 8000,
        |          977777]],
        |         "arrayArrayReal":[[1.111, 2.2], [9.139, 2.98, 4.67], [2.91, 2.2], [9.139, 2.98]],
        |         "arrayArrayDouble":[[1.111, 2.0, 4.67, 2.91, 2.2, 9.139, 2.98]],
        |         "arrayArrayString":[["Japan"], ["China"], ["India"]],
        |         "arrayArrayBoolean":[[false], [true], [false]]
        |        }   """.stripMargin

    val mySchema =
      """ {
        |	"name": "address",
        |	"type": "record",
        |	"fields": [
        |  {
        |			"name": "arrayArrayInt",
        |			"type": {
        |				"type": "array",
        |				"items": {
        |					"name": "FloorNum",
        |					"type": "array",
        |					"items": {
        |							"name": "EachdoorNums",
        |							"type": "int"
        |						}
        |				}
        |			}
        |		},
        |  {
        |			"name": "arrayArrayBigInt",
        |			"type": {
        |				"type": "array",
        |				"items": {
        |					"name": "FloorNum",
        |					"type": "array",
        |					"items": {
        |							"name": "EachdoorNums",
        |							"type": "long"
        |						}
        |				}
        |			}
        |		},
        |  {
        |			"name": "arrayArrayReal",
        |			"type": {
        |				"type": "array",
        |				"items": {
        |					"name": "FloorNum",
        |					"type": "array",
        |					"items": {
        |							"name": "EachdoorNums",
        |							"type": "float"
        |						}
        |				}
        |			}
        |		},
        |  {
        |			"name": "arrayArrayDouble",
        |			"type": {
        |				"type": "array",
        |				"items": {
        |					"name": "FloorNum",
        |					"type": "array",
        |					"items": {
        |							"name": "EachdoorNums",
        |							"type": "double"
        |						}
        |				}
        |			}
        |		},
        |  {
        |			"name": "arrayArrayString",
        |			"type": {
        |				"type": "array",
        |				"items": {
        |					"name": "FloorNum",
        |					"type": "array",
        |					"items": {
        |							"name": "EachdoorNums",
        |							"type": "string"
        |						}
        |				}
        |			}
        |		},
        |  {
        |			"name": "arrayArrayBoolean",
        |			"type": {
        |				"type": "array",
        |				"items": {
        |					"name": "FloorNum",
        |					"type": "array",
        |					"items": {
        |							"name": "EachdoorNums",
        |							"type": "boolean"
        |						}
        |				}
        |			}
        |		}
        |	]
        |} """.stripMargin

    val nn = new avro.Schema.Parser().parse(mySchema)
    val record1 = jsonToAvro(json1, mySchema)
    val record2 = jsonToAvro(json2, mySchema)
    val record3 = jsonToAvro(json3, mySchema)
    val record4 = jsonToAvro(json4, mySchema)
    var writerPath = new File(this.getClass.getResource("/").getPath
                              + "../../target/store/sdk_output/files2")
      .getCanonicalPath
    //getCanonicalPath gives path with \, but the code expects /.
    writerPath = writerPath.replace("\\", "/")
    try {
      val writer = CarbonWriter.builder
        .outputPath(writerPath)
        .enableLocalDictionary(false)
        .uniqueIdentifier(System.currentTimeMillis())
        .withAvroInput(nn)
        .writtenBy("GenerateFiles")
        .build()
      writer.write(record1)
      writer.write(record2)
      writer.write(record3)
      writer.write(record4)
      writer.close()
    } catch {
      case e: Exception =>
        Assert.fail(e.getMessage)
    }
  }

  def threeLevelArrayFile() = {
    val json1 =
      """ {
        | "array3_Int": [[[1,2,3], [4,5]], [[6,7,8], [9]], [[1,2], [4,5]]],
        | "array3_BigInt":[[[90000,600000000],[8000]],[[911111111]]],
        | "array3_Real":[[[1.111,2.2], [9.139,2.98]]],
        | "array3_Double":[[[1.111,2.2]], [[9.139,2.98989898]]],
        | "array3_String":[[["Japan", "China"], ["Brazil", "Paris"]], [["India"]]],
        | "array3_Boolean":[[[false, false], [false]], [[true]]]
        | } """.stripMargin
    val json2 =
      """ {
        | "array3_Int": [[[1,2,3], [0,5], [1,2,3,4,5], [4,5]]],
        | "array3_BigInt":[[[40000,600000000,8000],[9111111111]]],
        | "array3_Real":[[[1.111,2.2], [9.139,2.98]], [[9.99]]],
        | "array3_Double":[[[1.111,2.2], [9.139777,2.98]], [[9.99888]]],
        | "array3_String":[[["China", "Brazil"], ["Paris", "France"]]],
        | "array3_Boolean":[[[false], [true, false]]]
        | } """.stripMargin
    val json3 =
      """ {
        | "array3_Int": [[[1],[0],[3]],[[4,5]]],
        | "array3_BigInt":[[[5000],[600000000],[8000,9111111111],[20000],[600000000,8000,
        | 9111111111]]],
        | "array3_Real":[[[9.198]]],
        | "array3_Double":[[[0.1987979]]],
        | "array3_String":[[["Japan", "China", "India"]]],
        | "array3_Boolean":[[[false, true, false]]]
        | } """.stripMargin
    val json4 =
      """ {
        | "array3_Int": [[[0,9,0,1,3,2,3,4,7]]],
        | "array3_BigInt":[[[5000,600087000,8000,9111111111,20000,600000000,8000,977777]]],
        | "array3_Real":[[[1.111,2.2], [9.139,2.98,4.67]], [[2.91,2.2], [9.139,2.98]]],
        | "array3_Double":[[[1.111,2,4.67, 2.91,2.2, 9.139,2.98]]],
        | "array3_String":[[["Japan"], ["China"], ["India"]]],
        | "array3_Boolean":[[[false], [true], [false]]]
        | } """.stripMargin

    val mySchema =
      """ {
        |	"name": "address",
        |	"type": "record",
        |	"fields": [
        |  {
        |			"name": "array3_Int",
        |			"type": {
        |				"type": "array",
        |				"items": {
        |					"type": "array",
        |					"items": {
        |     					"type": "array",
        |          		"items": {
        |							"type": "int"
        |              }
        |						}
        |				}
        |			}
        |		},
        |  {
        |			"name": "array3_BigInt",
        |			"type": {
        |				"type": "array",
        |				"items": {
        |					"type": "array",
        |					"items": {
        |     					"type": "array",
        |          		"items": {
        |							"type": "long"
        |              }
        |						}
        |				}
        |			}
        |		},
        |  {
        |			"name": "array3_Real",
        |			"type": {
        |				"type": "array",
        |				"items": {
        |					"type": "array",
        |					"items": {
        |     					"type": "array",
        |          		"items": {
        |							"type": "float"
        |              }
        |						}
        |				}
        |			}
        |		},
        |  {
        |			"name": "array3_Double",
        |			"type": {
        |				"type": "array",
        |				"items": {
        |					"type": "array",
        |					"items": {
        |     					"type": "array",
        |          		"items": {
        |							"type": "double"
        |              }
        |						}
        |				}
        |			}
        |		},
        |  {
        |			"name": "array3_String",
        |			"type": {
        |				"type": "array",
        |				"items": {
        |					"type": "array",
        |					"items": {
        |     					"type": "array",
        |          		"items": {
        |							"type": "string"
        |              }
        |						}
        |				}
        |			}
        |		},
        |  {
        |			"name": "array3_Boolean",
        |			"type": {
        |				"type": "array",
        |				"items": {
        |					"type": "array",
        |					"items": {
        |     					"type": "array",
        |          		"items": {
        |							"type": "boolean"
        |              }
        |						}
        |				}
        |			}
        |		}
        |	]
        |} """.stripMargin

    val nn = new avro.Schema.Parser().parse(mySchema)
    val record1 = jsonToAvro(json1, mySchema)
    val record2 = jsonToAvro(json2, mySchema)
    val record3 = jsonToAvro(json3, mySchema)
    val record4 = jsonToAvro(json4, mySchema)
    var writerPath = new File(this.getClass.getResource("/").getPath
                              + "../../target/store/sdk_output/files3")
      .getCanonicalPath
    //getCanonicalPath gives path with \, but the code expects /.
    writerPath = writerPath.replace("\\", "/")
    try {
      val writer = CarbonWriter.builder
        .outputPath(writerPath)
        .enableLocalDictionary(false)
        .uniqueIdentifier(System.currentTimeMillis())
        .withAvroInput(nn)
        .writtenBy("GenerateFiles")
        .build()
      writer.write(record1)
      writer.write(record2)
      writer.write(record3)
      writer.write(record4)
      writer.close()
    } catch {
      case e: Exception =>
        Assert.fail(e.getMessage)
    }
  }

  def jsonToAvro(json: String, avroSchema: String): GenericRecord = {
    var input: InputStream = null
    var writer: DataFileWriter[GenericRecord] = null
    var encoder: Encoder = null
    var output: ByteArrayOutputStream = null
    try {
      val schema = new org.apache.avro.Schema.Parser().parse(avroSchema)
      val reader = new GenericDatumReader[GenericRecord](schema)
      input = new ByteArrayInputStream(json.getBytes())
      output = new ByteArrayOutputStream()
      val din = new DataInputStream(input)
      writer = new DataFileWriter[GenericRecord](new GenericDatumWriter[GenericRecord]())
      writer.create(schema, output)
      val decoder = DecoderFactory.get().jsonDecoder(schema, din)
      var datum: GenericRecord = reader.read(null, decoder)
      return datum
    } finally {
      input.close()
      writer.close()
    }
  }
}
