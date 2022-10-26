package com.happy.connectors.file;

import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.impl.StreamFormatAdapter;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.reader.SimpleStreamFormat;
import org.apache.flink.connector.file.src.reader.StreamFormat;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.csv.CsvReaderFormat;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;

/**
 * @author DeveloperZJQ
 * @since 2022/10/24
 */
public class FileSourceOperator {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = new StreamExecutionEnvironment();
        // 下面这两个api基本上见名之意
//        FileSource.forRecordStreamFormat();
//        FileSource.forBulkFileFormat();

        // source1
        final FileSource<String> source1 = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path("filepath"))
                .monitorContinuously(Duration.ofMillis(5))
                .build();

        // source2
        final FileSource<byte[]> source2 = FileSource.forRecordStreamFormat(new ArrayReaderFormat(), new Path("filepath")).build();

        // source3
        CsvReaderFormat<FileSourcePojo> csvFormat = CsvReaderFormat.forPojo(FileSourcePojo.class);
        FileSource<FileSourcePojo> source3 = FileSource.forRecordStreamFormat(csvFormat, Path.fromLocalFile(new File("filepath"))).build();

        // source4
        CsvReaderFormat<FileSourcePojo> csvReaderFormat = CsvReaderFormat.forSchema(CsvSchema.emptySchema(), TypeInformation.of(FileSourcePojo.class));
        FileSource<FileSourcePojo> source4 = FileSource.forRecordStreamFormat(csvReaderFormat, Path.fromLocalFile(new File("filepath"))).build();

        // bulk source5
        BulkFormat<FileSourcePojo, FileSourceSplit> bulkFormat = new StreamFormatAdapter<>(CsvReaderFormat.forPojo(FileSourcePojo.class));


        environment.execute(FileSourceOperator.class.getName());
    }

    private static final class ArrayReaderFormat extends SimpleStreamFormat<byte[]> {
        private static final long serialVersionUID = 1L;

        @Override
        public Reader<byte[]> createReader(Configuration config, FSDataInputStream stream) {
            return new ArrayReader(stream);
        }

        @Override
        public TypeInformation<byte[]> getProducedType() {
            return PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO;
        }

        private static final class ArrayReader implements StreamFormat.Reader<byte[]> {

            private static final int ARRAY_SIZE = 1 << 20; // 1 MiByte

            private final FSDataInputStream in;

            ArrayReader(FSDataInputStream in) {
                this.in = in;
            }

            @Nullable
            @Override
            public byte[] read() throws IOException {
                final byte[] array = new byte[ARRAY_SIZE];
                final int read = in.read(array);
                if (read == array.length) {
                    return array;
                } else if (read == -1) {
                    return null;
                } else {
                    return Arrays.copyOf(array, read);
                }
            }

            @Override
            public void close() throws IOException {
                in.close();
            }
        }
    }

}