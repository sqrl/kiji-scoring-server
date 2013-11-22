package org.kiji.scoring.server

import org.kiji.schema.testutil.AbstractKijiIntegrationTest
import org.junit.{Test, Before}
import org.apache.hadoop.fs.Path
import java.io.File
import org.apache.avro.Schema
import com.google.common.collect.Lists
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.scalatest.junit.JUnitSuite

/**
 * Created with IntelliJ IDEA.
 * User: milo
 * Date: 12/2/13
 * Time: 3:55 PM
 * To change this template use File | Settings | File Templates.
 */
class IntegrationTestScoringServerKVStore extends AbstractKijiIntegrationTest with JUnitSuite {

  /** Writes an avro file of generic records with a 'key', 'blah', and 'value' field. */
  private def writeGenericRecordAvroFile: Path = {
    val file: File = new File(getTempDir, "generic-kv.avro")
    val writerSchema: Schema = Schema.createRecord("record", null, null, false)
    writerSchema.setFields(Lists.newArrayList(
      new Schema.Field("key", Schema.create(Schema.Type.STRING), null, null),
      new Schema.Field("value", Schema.create(Schema.Type.INT), null, null)))
    val fileWriter: DataFileWriter[GenericRecord] =
        new DataFileWriter[GenericRecord](
            new GenericDatumWriter[GenericRecord](writerSchema))
        .create(writerSchema, file)
    try {
      var record: GenericData.Record = new GenericData.Record(writerSchema)
      record.put("key", "one")
      record.put("value", 1)
      fileWriter.append(record)
      record = new GenericData.Record(writerSchema)
      record.put("key", "two")
      record.put("value", 2)
      fileWriter.append(record)
    }
    finally {
      fileWriter.close
    }
    getDfsPath(file)
  }

  @Test
  def setup() {
    println(writeGenericRecordAvroFile.toString())
  }
}
