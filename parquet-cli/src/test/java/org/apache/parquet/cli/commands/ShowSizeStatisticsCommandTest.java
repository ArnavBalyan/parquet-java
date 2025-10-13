/*
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
package org.apache.parquet.cli.commands;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.crypto.ColumnEncryptionProperties;
import org.apache.parquet.crypto.FileEncryptionProperties;
import org.apache.parquet.crypto.ParquetCipher;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.junit.Assert;
import org.junit.Test;

public class ShowSizeStatisticsCommandTest extends ParquetFileTest {
  @Test
  public void testShowSizeStatisticsCommand() throws IOException {
    File file = parquetFile();
    ShowSizeStatisticsCommand command = new ShowSizeStatisticsCommand(createLogger());
    command.targets = Arrays.asList(file.getAbsolutePath());
    command.setConf(new Configuration());
    Assert.assertEquals(0, command.run());
  }

  @Test
  public void testShowSizeStatisticsWithColumnFilter() throws IOException {
    File file = parquetFile();
    ShowSizeStatisticsCommand command = new ShowSizeStatisticsCommand(createLogger());
    command.targets = Arrays.asList(file.getAbsolutePath());
    command.columns = Arrays.asList(INT32_FIELD, INT64_FIELD);
    command.setConf(new Configuration());
    Assert.assertEquals(0, command.run());
  }

  @Test
  public void testShowSizeStatisticsWithRowGroupFilter() throws IOException {
    File file = parquetFile();
    ShowSizeStatisticsCommand command = new ShowSizeStatisticsCommand(createLogger());
    command.targets = Arrays.asList(file.getAbsolutePath());
    command.rowGroups = Arrays.asList(0);
    command.setConf(new Configuration());
    Assert.assertEquals(0, command.run());
  }

  @Test
  public void testEncryptedFileWithSizeStatistics() throws IOException {
    File encryptedFile = createEncryptedFile();

    ShowSizeStatisticsCommand command = new ShowSizeStatisticsCommand(createLogger());
    command.targets = Arrays.asList(encryptedFile.getAbsolutePath());

    Configuration conf = new Configuration();
    conf.set("parquet.encryption.footer.key", "0102030405060708090a0b0c0d0e0f10");
    conf.set("parquet.encryption.column.keys", "02030405060708090a0b0c0d0e0f1011:name,email");
    command.setConf(conf);

    Assert.assertEquals(0, command.run());

    encryptedFile.delete();
  }

  private File createEncryptedFile() throws IOException {
    MessageType schema = Types.buildMessage()
        .required(INT32)
        .named("id")
        .required(BINARY)
        .named("name")
        .required(BINARY)
        .named("email")
        .named("test_schema");

    File tempFile = new File(getTempFolder(), "encrypted_size_stats_test.parquet");
    tempFile.deleteOnExit();

    Configuration conf = new Configuration();
    GroupWriteSupport.setSchema(schema, conf);

    String[] encryptColumns = {"name", "email"};
    FileEncryptionProperties encryptionProperties =
        createFileEncryptionProperties(encryptColumns, ParquetCipher.AES_GCM_CTR_V1, true);

    SimpleGroupFactory factory = new SimpleGroupFactory(schema);

    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new Path(tempFile.toURI()))
        .withConf(conf)
        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
        .withEncryption(encryptionProperties)
        .withPageSize(1024)
        .withRowGroupSize(4096)
        .build()) {

      for (int i = 0; i < 10; i++) {
        SimpleGroup group = (SimpleGroup) factory.newGroup();
        group.add("id", i + 1);
        group.add("name", Binary.fromString("name_" + i));
        group.add("email", Binary.fromString("email_" + i + "@test.com"));
        writer.write(group);
      }
    }

    return tempFile;
  }

  private FileEncryptionProperties createFileEncryptionProperties(
      String[] encryptColumns, ParquetCipher cipher, boolean footerEncryption) {

    byte[] footerKey = {
      0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10
    };

    byte[] sharedKey = new byte[] {
      0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11
    };

    Map<ColumnPath, ColumnEncryptionProperties> columnPropertyMap = new HashMap<>();
    for (String columnPath : encryptColumns) {
      ColumnPath column = ColumnPath.fromDotString(columnPath);

      ColumnEncryptionProperties columnProps = ColumnEncryptionProperties.builder(column)
          .withKey(sharedKey)
          .withKeyMetaData(columnPath.getBytes(StandardCharsets.UTF_8))
          .build();
      columnPropertyMap.put(column, columnProps);
    }

    FileEncryptionProperties.Builder builder = FileEncryptionProperties.builder(footerKey)
        .withFooterKeyMetadata("footkey".getBytes(StandardCharsets.UTF_8))
        .withAlgorithm(cipher)
        .withEncryptedColumns(columnPropertyMap);

    if (!footerEncryption) {
      builder.withPlaintextFooter();
    }

    return builder.build();
  }
}
