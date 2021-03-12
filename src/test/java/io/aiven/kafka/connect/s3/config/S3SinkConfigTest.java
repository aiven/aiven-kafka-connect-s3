/*
 * Copyright 2020 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.kafka.connect.s3.config;

import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigException;

import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.common.config.FormatType;
import io.aiven.kafka.connect.common.config.OutputField;
import io.aiven.kafka.connect.common.config.OutputFieldEncodingType;
import io.aiven.kafka.connect.common.config.OutputFieldType;
import io.aiven.kafka.connect.s3.OldFullKeyFormatters;

import com.amazonaws.regions.Regions;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import static io.aiven.kafka.connect.s3.config.S3SinkConfig.AWS_ACCESS_KEY_ID;
import static io.aiven.kafka.connect.s3.config.S3SinkConfig.AWS_ACCESS_KEY_ID_CONFIG;
import static io.aiven.kafka.connect.s3.config.S3SinkConfig.AWS_S3_BUCKET;
import static io.aiven.kafka.connect.s3.config.S3SinkConfig.AWS_S3_BUCKET_NAME_CONFIG;
import static io.aiven.kafka.connect.s3.config.S3SinkConfig.AWS_S3_ENDPOINT;
import static io.aiven.kafka.connect.s3.config.S3SinkConfig.AWS_S3_PREFIX;
import static io.aiven.kafka.connect.s3.config.S3SinkConfig.AWS_S3_PREFIX_CONFIG;
import static io.aiven.kafka.connect.s3.config.S3SinkConfig.AWS_S3_REGION;
import static io.aiven.kafka.connect.s3.config.S3SinkConfig.AWS_S3_REGION_CONFIG;
import static io.aiven.kafka.connect.s3.config.S3SinkConfig.AWS_SECRET_ACCESS_KEY;
import static io.aiven.kafka.connect.s3.config.S3SinkConfig.AWS_SECRET_ACCESS_KEY_CONFIG;
import static io.aiven.kafka.connect.s3.config.S3SinkConfig.FILE_COMPRESSION_TYPE_CONFIG;
import static io.aiven.kafka.connect.s3.config.S3SinkConfig.FORMAT_OUTPUT_FIELDS_CONFIG;
import static io.aiven.kafka.connect.s3.config.S3SinkConfig.OUTPUT_COMPRESSION;
import static io.aiven.kafka.connect.s3.config.S3SinkConfig.OUTPUT_FIELDS;
import static io.aiven.kafka.connect.s3.config.S3SinkConfig.TIMESTAMP_SOURCE;
import static io.aiven.kafka.connect.s3.config.S3SinkConfig.TIMESTAMP_TIMEZONE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class S3SinkConfigTest {

    @Test
    void correctFullConfig() {
        final var props = new HashMap<String, String>();

        props.put(S3SinkConfig.AWS_ACCESS_KEY_ID_CONFIG, "AWS_ACCESS_KEY_ID");
        props.put(S3SinkConfig.AWS_SECRET_ACCESS_KEY_CONFIG, "AWS_SECRET_ACCESS_KEY");
        props.put(S3SinkConfig.AWS_S3_BUCKET_NAME_CONFIG, "THE_BUCKET");
        props.put(S3SinkConfig.AWS_S3_ENDPOINT_CONFIG, "AWS_S3_ENDPOINT");
        props.put(S3SinkConfig.AWS_S3_PREFIX_CONFIG, "AWS_S3_PREFIX");
        props.put(S3SinkConfig.AWS_S3_REGION_CONFIG, Regions.US_EAST_1.getName());

        props.put(S3SinkConfig.FILE_COMPRESSION_TYPE_CONFIG, CompressionType.GZIP.name);
        props.put(
            S3SinkConfig.FORMAT_OUTPUT_FIELDS_CONFIG,
            Arrays.stream(OutputFieldType.values())
                .map(OutputFieldType::name)
                .map(String::toLowerCase)
                .collect(Collectors.joining(","))
        );
        props.put(S3SinkConfig.FORMAT_OUTPUT_FIELDS_VALUE_ENCODING_CONFIG, OutputFieldEncodingType.NONE.name);

        final var conf = new S3SinkConfig(props);
        final var awsCredentials = conf.getAwsCredentials();

        assertEquals("AWS_ACCESS_KEY_ID", awsCredentials.getAccessKeyId().value());
        assertEquals("AWS_SECRET_ACCESS_KEY", awsCredentials.getSecretAccessKey().value());
        assertEquals("THE_BUCKET", conf.getAwsS3BucketName());
        assertEquals("AWS_S3_PREFIX", conf.getAwsS3Prefix());
        assertEquals("AWS_S3_ENDPOINT", conf.getAwsS3EndPoint());
        assertEquals(Regions.US_EAST_1, conf.getAwsS3Region());
        assertEquals(CompressionType.GZIP, conf.getCompressionType());
        assertEquals(OutputFieldEncodingType.NONE, conf.getOutputFieldEncodingType());
        assertEquals(
            Arrays.asList(
                new OutputField(OutputFieldType.KEY, OutputFieldEncodingType.NONE),
                new OutputField(OutputFieldType.VALUE, OutputFieldEncodingType.NONE),
                new OutputField(OutputFieldType.OFFSET, OutputFieldEncodingType.NONE),
                new OutputField(OutputFieldType.TIMESTAMP, OutputFieldEncodingType.NONE),
                new OutputField(OutputFieldType.HEADERS, OutputFieldEncodingType.NONE)
            ),
            conf.getOutputFields()
        );
        assertEquals(FormatType.forName("csv"), conf.getFormatType());
    }

    @Test
    void correctFullConfigForOldStyleConfigParameters() {
        final var props = new HashMap<String, String>();

        props.put(AWS_ACCESS_KEY_ID, "AWS_ACCESS_KEY_ID");
        props.put(AWS_SECRET_ACCESS_KEY, "AWS_SECRET_ACCESS_KEY");
        props.put(AWS_S3_BUCKET, "THE_BUCKET");
        props.put(AWS_S3_ENDPOINT, "AWS_S3_ENDPOINT");
        props.put(AWS_S3_PREFIX, "AWS_S3_PREFIX");
        props.put(AWS_S3_REGION, Regions.US_EAST_1.getName());

        props.put(OUTPUT_COMPRESSION, CompressionType.GZIP.name);
        props.put(
            OUTPUT_FIELDS,
            Arrays.stream(OutputFieldType.values())
                .map(OutputFieldType::name)
                .map(String::toLowerCase)
                .collect(Collectors.joining(","))
        );

        final var conf = new S3SinkConfig(props);
        final var awsCredentials = conf.getAwsCredentials();

        assertEquals("AWS_ACCESS_KEY_ID", awsCredentials.getAccessKeyId().value());
        assertEquals("AWS_SECRET_ACCESS_KEY", awsCredentials.getSecretAccessKey().value());
        assertEquals("THE_BUCKET", conf.getAwsS3BucketName());
        assertEquals("AWS_S3_PREFIX", conf.getAwsS3Prefix());
        assertEquals("AWS_S3_ENDPOINT", conf.getAwsS3EndPoint());
        assertEquals(Regions.US_EAST_1, conf.getAwsS3Region());
        assertEquals(CompressionType.GZIP, conf.getCompressionType());
        assertEquals(OutputFieldEncodingType.BASE64, conf.getOutputFieldEncodingType());
        assertEquals(
            Arrays.asList(
                new OutputField(OutputFieldType.KEY, OutputFieldEncodingType.BASE64),
                new OutputField(OutputFieldType.VALUE, OutputFieldEncodingType.BASE64),
                new OutputField(OutputFieldType.OFFSET, OutputFieldEncodingType.NONE),
                new OutputField(OutputFieldType.TIMESTAMP, OutputFieldEncodingType.NONE),
                new OutputField(OutputFieldType.HEADERS, OutputFieldEncodingType.NONE)
            ),
            conf.getOutputFields()
        );
    }

    @Test
    void newConfigurationPropertiesHaveHigherPriorityOverOldOne() {
        final var props = new HashMap<String, String>();

        props.put(S3SinkConfig.AWS_ACCESS_KEY_ID_CONFIG, "AWS_ACCESS_KEY_ID");
        props.put(S3SinkConfig.AWS_SECRET_ACCESS_KEY_CONFIG, "AWS_SECRET_ACCESS_KEY");
        props.put(S3SinkConfig.AWS_S3_BUCKET_NAME_CONFIG, "THE_BUCKET");
        props.put(S3SinkConfig.AWS_S3_ENDPOINT_CONFIG, "AWS_S3_ENDPOINT");
        props.put(S3SinkConfig.AWS_S3_PREFIX_CONFIG, "AWS_S3_PREFIX");
        props.put(S3SinkConfig.AWS_S3_REGION_CONFIG, Regions.US_EAST_1.getName());

        props.put(S3SinkConfig.FILE_COMPRESSION_TYPE_CONFIG, CompressionType.GZIP.name);
        props.put(
            S3SinkConfig.FORMAT_OUTPUT_FIELDS_CONFIG,
            Arrays.stream(OutputFieldType.values())
                .map(OutputFieldType::name)
                .map(String::toLowerCase)
                .collect(Collectors.joining(","))
        );
        props.put(S3SinkConfig.FORMAT_OUTPUT_FIELDS_VALUE_ENCODING_CONFIG, OutputFieldEncodingType.NONE.name);

        props.put(AWS_ACCESS_KEY_ID, "AWS_ACCESS_KEY_ID_#1");
        props.put(AWS_SECRET_ACCESS_KEY, "AWS_SECRET_ACCESS_KEY_#1");
        props.put(AWS_S3_BUCKET, "THE_BUCKET_#1");
        props.put(AWS_S3_ENDPOINT, "AWS_S3_ENDPOINT_#1");
        props.put(AWS_S3_PREFIX, "AWS_S3_PREFIX_#1");
        props.put(AWS_S3_REGION, Regions.US_WEST_1.getName());

        props.put(OUTPUT_COMPRESSION, CompressionType.NONE.name);
        props.put(OUTPUT_FIELDS, "key, value");

        final var conf = new S3SinkConfig(props);
        final var awsCredentials = conf.getAwsCredentials();

        assertEquals("AWS_ACCESS_KEY_ID", awsCredentials.getAccessKeyId().value());
        assertEquals("AWS_SECRET_ACCESS_KEY", awsCredentials.getSecretAccessKey().value());
        assertEquals("THE_BUCKET", conf.getAwsS3BucketName());
        assertEquals("AWS_S3_PREFIX", conf.getAwsS3Prefix());
        assertEquals("AWS_S3_ENDPOINT", conf.getAwsS3EndPoint());
        assertEquals(Regions.US_EAST_1, conf.getAwsS3Region());
        assertEquals(CompressionType.GZIP, conf.getCompressionType());
        assertEquals(OutputFieldEncodingType.NONE, conf.getOutputFieldEncodingType());
        assertEquals(
            Arrays.asList(
                new OutputField(OutputFieldType.KEY, OutputFieldEncodingType.NONE),
                new OutputField(OutputFieldType.VALUE, OutputFieldEncodingType.NONE),
                new OutputField(OutputFieldType.OFFSET, OutputFieldEncodingType.NONE),
                new OutputField(OutputFieldType.TIMESTAMP, OutputFieldEncodingType.NONE),
                new OutputField(OutputFieldType.HEADERS, OutputFieldEncodingType.NONE)
            ),
            conf.getOutputFields()
        );
    }

    @Test
    final void emptyAwsAccessKeyID() {
        final Map<String, String> props = new HashMap<>();
        props.put(AWS_ACCESS_KEY_ID, "");
        Throwable t = assertThrows(
            ConfigException.class,
            () -> new S3SinkConfig(props)
        );
        assertEquals(
            "Invalid value [hidden] for configuration aws_access_key_id: Password must be non-empty",
            t.getMessage()
        );

        props.put(S3SinkConfig.AWS_ACCESS_KEY_ID_CONFIG, "");
        t = assertThrows(
            ConfigException.class,
            () -> new S3SinkConfig(props)
        );
        assertEquals(
            "Invalid value [hidden] for configuration aws.access.key.id: Password must be non-empty",
            t.getMessage()
        );
    }

    @Test
    final void emptyAwsSecretAccessKey() {
        final Map<String, String> props = new HashMap<>();
        props.put(AWS_ACCESS_KEY_ID, "blah-blah-blah");
        props.put(AWS_SECRET_ACCESS_KEY, "");

        Throwable t = assertThrows(
            ConfigException.class,
            () -> new S3SinkConfig(props)
        );
        assertEquals(
            "Invalid value [hidden] for configuration aws_secret_access_key: Password must be non-empty",
            t.getMessage()
        );

        props.put(S3SinkConfig.AWS_ACCESS_KEY_ID_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_SECRET_ACCESS_KEY_CONFIG, "");
        t = assertThrows(
            ConfigException.class,
            () -> new S3SinkConfig(props)
        );
        assertEquals(
            "Invalid value [hidden] for configuration aws.secret.access.key: Password must be non-empty",
            t.getMessage()
        );
    }

    @Test
    final void emptyAwsS3Bucket() {
        final Map<String, String> props = new HashMap<>();
        props.put(AWS_ACCESS_KEY_ID, "blah-blah-blah");
        props.put(AWS_SECRET_ACCESS_KEY, "blah-blah-blah");
        props.put(AWS_S3_BUCKET, "");

        Throwable t = assertThrows(
            ConfigException.class,
            () -> new S3SinkConfig(props)
        );
        assertEquals(
            "Invalid value  for configuration aws_s3_bucket: String must be non-empty",
            t.getMessage()
        );

        props.put(S3SinkConfig.AWS_ACCESS_KEY_ID_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_SECRET_ACCESS_KEY_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_S3_BUCKET_NAME_CONFIG, "");
        t = assertThrows(
            ConfigException.class,
            () -> new S3SinkConfig(props)
        );
        assertEquals(
            "Invalid value  for configuration aws.s3.bucket.name: String must be non-empty",
            t.getMessage()
        );
    }

    @Test
    final void emptyAwsS3Region() {
        final Map<String, String> props = new HashMap<>();
        props.put(AWS_ACCESS_KEY_ID, "blah-blah-blah");
        props.put(AWS_SECRET_ACCESS_KEY, "blah-blah-blah");
        props.put(AWS_S3_BUCKET, "blah-blah-blah");
        props.put(AWS_S3_REGION, "");
        Throwable t = assertThrows(
            ConfigException.class,
            () -> new S3SinkConfig(props)
        );
        assertEquals(
            "Invalid value  for configuration aws_s3_region: "
                + "supported values are: "
                + Arrays.stream(Regions.values()).map(Regions::getName).collect(Collectors.joining(", ")),
            t.getMessage()
        );

        props.put(S3SinkConfig.AWS_ACCESS_KEY_ID_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_SECRET_ACCESS_KEY_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_S3_BUCKET_NAME_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_S3_REGION_CONFIG, "");
        t = assertThrows(
            ConfigException.class,
            () -> new S3SinkConfig(props)
        );
        assertEquals(
            "Invalid value  for configuration aws.s3.region: "
                + "supported values are: "
                + Arrays.stream(Regions.values()).map(Regions::getName).collect(Collectors.joining(", ")),
            t.getMessage()
        );
    }

    @Test
    final void emptyAwsS3Prefix() {
        final Map<String, String> props = new HashMap<>();
        props.put(AWS_ACCESS_KEY_ID, "blah-blah-blah");
        props.put(AWS_SECRET_ACCESS_KEY, "blah-blah-blah");
        props.put(AWS_S3_BUCKET, "blah-blah-blah");
        props.put(AWS_S3_REGION, Regions.US_WEST_1.getName());
        props.put(AWS_S3_PREFIX, "");
        Throwable t = assertThrows(
            ConfigException.class,
            () -> new S3SinkConfig(props)
        );
        assertEquals(
            "Invalid value  for configuration aws_s3_prefix: String must be non-empty",
            t.getMessage()
        );

        props.put(S3SinkConfig.AWS_ACCESS_KEY_ID_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_SECRET_ACCESS_KEY_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_S3_BUCKET_NAME_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_S3_REGION_CONFIG, Regions.US_WEST_1.getName());
        props.put(S3SinkConfig.AWS_S3_PREFIX_CONFIG, "");
        t = assertThrows(
            ConfigException.class,
            () -> new S3SinkConfig(props)
        );
        assertEquals(
            "Invalid value  for configuration aws.s3.prefix: String must be non-empty",
            t.getMessage()
        );
    }

    @Test
    final void emptyAwsS3EndPoint() {
        final Map<String, String> props = new HashMap<>();
        props.put(AWS_ACCESS_KEY_ID, "blah-blah-blah");
        props.put(AWS_SECRET_ACCESS_KEY, "blah-blah-blah");
        props.put(AWS_S3_BUCKET, "blah-blah-blah");
        props.put(AWS_S3_REGION, Regions.US_EAST_1.getName());
        props.put(AWS_S3_PREFIX, "blah-blah-blah");
        props.put(AWS_S3_ENDPOINT, "");
        Throwable t = assertThrows(
            ConfigException.class,
            () -> new S3SinkConfig(props)
        );
        assertEquals(
            "Invalid value  for configuration aws_s3_endpoint: String must be non-empty",
            t.getMessage()
        );

        props.put(S3SinkConfig.AWS_ACCESS_KEY_ID_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_SECRET_ACCESS_KEY_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_S3_BUCKET_NAME_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_S3_REGION_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_S3_PREFIX_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_S3_ENDPOINT_CONFIG, "");
        t = assertThrows(
            ConfigException.class,
            () -> new S3SinkConfig(props)
        );
        assertEquals(
            "Invalid value  for configuration aws.s3.endpoint: String must be non-empty",
            t.getMessage()
        );
    }

    @Test
    final void wrongAwsS3EndPoint() {
        final Map<String, String> props = new HashMap<>();
        props.put(AWS_ACCESS_KEY_ID, "blah-blah-blah");
        props.put(AWS_SECRET_ACCESS_KEY, "blah-blah-blah");
        props.put(AWS_S3_BUCKET, "blah-blah-blah");
        props.put(AWS_S3_REGION, Regions.US_EAST_1.getName());
        props.put(AWS_S3_PREFIX, "blah-blah-blah");
        props.put(AWS_S3_ENDPOINT, "ffff://asdsadas");
        Throwable t = assertThrows(
            ConfigException.class,
            () -> new S3SinkConfig(props)
        );
        assertEquals(
            "Invalid value ffff://asdsadas for configuration aws_s3_endpoint: should be valid URL",
            t.getMessage()
        );

        props.put(S3SinkConfig.AWS_ACCESS_KEY_ID_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_SECRET_ACCESS_KEY_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_S3_BUCKET_NAME_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_S3_REGION_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_S3_PREFIX_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_S3_ENDPOINT_CONFIG, "ffff://asdsadas");
        t = assertThrows(
            ConfigException.class,
            () -> new S3SinkConfig(props)
        );
        assertEquals(
            "Invalid value ffff://asdsadas for configuration aws.s3.endpoint: should be valid URL",
            t.getMessage()
        );
    }


    @Test
    void emptyOutputField() {
        final Map<String, String> props = new HashMap<>();
        props.put(S3SinkConfig.AWS_ACCESS_KEY_ID_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_SECRET_ACCESS_KEY_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_S3_BUCKET_NAME_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_S3_REGION_CONFIG, Regions.US_WEST_1.getName());
        props.put(S3SinkConfig.AWS_S3_PREFIX_CONFIG, "blah-blah-blah");
        props.put(OUTPUT_FIELDS, "");

        Throwable t = assertThrows(
            ConfigException.class, () -> new S3SinkConfig(props)
        );
        assertEquals(
            "Invalid value [] for configuration output_fields: cannot be empty",
            t.getMessage()
        );

        props.remove(OUTPUT_FIELDS);
        props.put(S3SinkConfig.FORMAT_OUTPUT_FIELDS_CONFIG, "");

        t = assertThrows(
            ConfigException.class, () -> new S3SinkConfig(props)
        );
        assertEquals(
            "Invalid value [] for configuration format.output.fields: cannot be empty",
            t.getMessage()
        );
    }

    @Test
    void supportPriorityForOutputFields() {
        final var props = Maps.<String, String>newHashMap();
        props.put(S3SinkConfig.AWS_ACCESS_KEY_ID_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_SECRET_ACCESS_KEY_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_S3_BUCKET_NAME_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_S3_REGION_CONFIG, Regions.US_WEST_1.getName());
        props.put(S3SinkConfig.AWS_S3_PREFIX_CONFIG, "blah-blah-blah");

        props.put(OUTPUT_FIELDS, "key,value,offset,timestamp");
        props.put(S3SinkConfig.FORMAT_OUTPUT_FIELDS_CONFIG, "key");

        final var conf = new S3SinkConfig(props);

        assertEquals(
            List.of(
                new OutputField(OutputFieldType.KEY, OutputFieldEncodingType.BASE64)
            ),
            conf.getOutputFields()
        );
    }

    @Test
    void unsupportedOutputField() {
        final Map<String, String> props = new HashMap<>();
        props.put(S3SinkConfig.AWS_ACCESS_KEY_ID_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_SECRET_ACCESS_KEY_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_S3_BUCKET_NAME_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_S3_REGION_CONFIG, Regions.US_WEST_1.getName());
        props.put(OUTPUT_FIELDS, "key,value,offset,timestamp,unsupported");

        Throwable t = assertThrows(
            ConfigException.class, () -> new S3SinkConfig(props)
        );
        assertEquals(
            "Invalid value [key, value, offset, timestamp, unsupported] "
                + "for configuration output_fields: "
                + "supported values are: 'key', 'value', 'offset', 'timestamp', 'headers'",
            t.getMessage()
        );

        props.remove(OUTPUT_FIELDS);
        props.put(S3SinkConfig.FORMAT_OUTPUT_FIELDS_CONFIG, "key,value,offset,timestamp,unsupported");

        t = assertThrows(
            ConfigException.class, () -> new S3SinkConfig(props)
        );
        assertEquals(
            "Invalid value [key, value, offset, timestamp, unsupported] "
                + "for configuration format.output.fields: "
                + "supported values are: 'key', 'value', 'offset', 'timestamp', 'headers'",
            t.getMessage()
        );
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"none", "gzip", "snappy", "zstd"})
    void supportedCompression(final String compression) {
        final Map<String, String> props = new HashMap<>();
        props.put(S3SinkConfig.AWS_ACCESS_KEY_ID_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_SECRET_ACCESS_KEY_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_S3_BUCKET_NAME_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_S3_REGION_CONFIG, Regions.US_WEST_1.getName());
        props.put(S3SinkConfig.AWS_S3_PREFIX_CONFIG, "blah-blah-blah");
        if (!Objects.isNull(compression)) {
            props.put(OUTPUT_COMPRESSION, compression);
        }

        var config = new S3SinkConfig(props);
        assertEquals(
            determineExpectedCompressionType(compression),
            config.getCompressionType()
        );

        props.remove(OUTPUT_COMPRESSION);
        if (!Objects.isNull(compression)) {
            props.put(FILE_COMPRESSION_TYPE_CONFIG, compression);
        }

        config = new S3SinkConfig(props);
        assertEquals(
            determineExpectedCompressionType(compression),
            config.getCompressionType()
        );
    }

    @Test
    void supportPriorityForCompressionTypeConfig() {
        final Map<String, String> props = new HashMap<>();
        props.put(S3SinkConfig.AWS_ACCESS_KEY_ID_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_SECRET_ACCESS_KEY_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_S3_BUCKET_NAME_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_S3_REGION_CONFIG, Regions.US_WEST_1.getName());
        props.put(S3SinkConfig.AWS_S3_PREFIX_CONFIG, "blah-blah-blah");
        props.put(OUTPUT_COMPRESSION, CompressionType.GZIP.name);
        props.put(S3SinkConfig.FILE_COMPRESSION_TYPE_CONFIG, CompressionType.NONE.name);

        final var config = new S3SinkConfig(props);

        assertEquals(CompressionType.NONE, config.getCompressionType());
    }

    private CompressionType determineExpectedCompressionType(final String compression) {
        if (Objects.isNull(compression) || "gzip".equals(compression)) {
            return CompressionType.GZIP;
        } else if ("none".equals(compression)) {
            return CompressionType.NONE;
        } else if ("snappy".equals(compression)) {
            return CompressionType.SNAPPY;
        } else if ("zstd".equals(compression)) {
            return CompressionType.ZSTD;
        } else {
            throw new RuntimeException("Shouldn't be here");
        }
    }

    @Test
    void unsupportedCompressionType() {
        final Map<String, String> props = new HashMap<>();
        props.put(S3SinkConfig.AWS_ACCESS_KEY_ID_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_SECRET_ACCESS_KEY_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_S3_BUCKET_NAME_CONFIG, "blah-blah-blah");
        props.put(S3SinkConfig.AWS_S3_REGION_CONFIG, Regions.US_WEST_1.getName());
        props.put(S3SinkConfig.FORMAT_OUTPUT_FIELDS_CONFIG, "key,value,offset,timestamp");
        props.put(OUTPUT_COMPRESSION, "unsupported");

        Throwable t = assertThrows(
            ConfigException.class, () -> new S3SinkConfig(props)
        );
        assertEquals(
            "Invalid value unsupported for configuration output_compression: "
                + "supported values are: 'none', 'gzip', 'snappy', 'zstd'",
            t.getMessage());

        props.remove(OUTPUT_COMPRESSION);
        props.put(S3SinkConfig.FILE_COMPRESSION_TYPE_CONFIG, "unsupported");

        t = assertThrows(
            ConfigException.class, () -> new S3SinkConfig(props)
        );
        assertEquals(
            "Invalid value unsupported for configuration file.compression.type: "
                + "supported values are: 'none', 'gzip', 'snappy', 'zstd'",
            t.getMessage());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldBuildPrefixTemplate(final boolean yyyyUppercase) {
        final String prefix;
        if (yyyyUppercase) {
            prefix = "{{timestamp:unit=YYYY}}/{{timestamp:unit=MM}}/{{timestamp:unit=dd}}/";
        } else {
            prefix = "{{timestamp:unit=yyyy}}/{{timestamp:unit=MM}}/{{timestamp:unit=dd}}/";
        }

        final Map<String, String> props = new HashMap<>();
        props.put(AWS_ACCESS_KEY_ID_CONFIG, "blah-blah-blah");
        props.put(AWS_SECRET_ACCESS_KEY_CONFIG, "blah-blah-blah");
        props.put(AWS_S3_BUCKET_NAME_CONFIG, "blah-blah-blah");
        props.put(AWS_S3_REGION_CONFIG, Regions.US_WEST_1.getName());
        props.put(FORMAT_OUTPUT_FIELDS_CONFIG, "key,value,offset,timestamp,headers");
        props.put(TIMESTAMP_TIMEZONE, "Europe/Berlin");
        props.put(TIMESTAMP_SOURCE, "wallclock");
        props.put(AWS_S3_PREFIX_CONFIG, prefix);

        final var config = new S3SinkConfig(props);

        final var expectedTimestamp = config.getTimestampSource().time();

        final var renderedPrefix =
            config.getPrefixTemplate()
                .instance()
                .bindVariable("timestamp", parameter ->
                    OldFullKeyFormatters.TIMESTAMP.apply(config.getTimestampSource(), parameter))
                .render();

        assertEquals(
            String.format(
                "%s/%s/%s/",
                expectedTimestamp.format(DateTimeFormatter.ofPattern("yyyy")),
                expectedTimestamp.format(DateTimeFormatter.ofPattern("MM")),
                expectedTimestamp.format(DateTimeFormatter.ofPattern("dd"))
            ),
            renderedPrefix
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {"jsonl", "json", "csv"})
    void supportedFormatTypeConfig(final String formatType) {
        final Map<String, String> properties = new HashMap<>();
        properties.put(S3SinkConfig.AWS_ACCESS_KEY_ID_CONFIG, "any_access_key_id");
        properties.put(S3SinkConfig.AWS_SECRET_ACCESS_KEY_CONFIG, "any_secret_key");
        properties.put(S3SinkConfig.AWS_S3_BUCKET_NAME_CONFIG, "any_bucket");
        properties.put(S3SinkConfig.AWS_S3_PREFIX_CONFIG, "any_prefix");
        properties.put(S3SinkConfig.FORMAT_OUTPUT_TYPE_CONFIG, formatType);


        final S3SinkConfig c = new S3SinkConfig(properties);
        final FormatType expectedFormatType = FormatType.forName(formatType);

        assertEquals(expectedFormatType, c.getFormatType());
    }

    @Test
    void wrongFormatTypeConfig() {
        final Map<String, String> properties = new HashMap<>();
        properties.put(S3SinkConfig.FORMAT_OUTPUT_TYPE_CONFIG, "unknown");

        final Throwable t = assertThrows(
            ConfigException.class,
            () -> new S3SinkConfig(properties)
        );
        assertEquals(
            "Invalid value unknown for configuration format.output.type: "
                + "supported values are: 'csv', 'json', 'jsonl', 'parquet'", t.getMessage());

    }

    @Test
    void notSupportYyyyUppercaseInFilenameTemplate() {
        final Map<String, String> properties =
            Map.of(
                S3SinkConfig.FILE_NAME_TEMPLATE_CONFIG,
                "{{topic}}-"
                    + "{{timestamp:unit=YYYY}}"
                    + "-{{partition}}-{{start_offset:padding=true}}.gz",
                S3SinkConfig.AWS_ACCESS_KEY_ID_CONFIG, "any_access_key_id",
                S3SinkConfig.AWS_SECRET_ACCESS_KEY_CONFIG, "any_secret_key",
                S3SinkConfig.AWS_S3_BUCKET_NAME_CONFIG, "any_bucket"
            );
        final Throwable t = assertThrows(
            ConfigException.class,
            () -> new S3SinkConfig(properties)
        );
        assertEquals(
            "Invalid value {{topic}}-{{timestamp:unit=YYYY}}-{{partition}}-{{start_offset:padding=true}}.gz "
                + "for configuration file.name.template: unsupported set of template variables parameters, "
                + "supported sets are: start_offset:padding=true|false,timestamp:unit=yyyy|MM|dd|HH",
            t.getMessage());
    }

    @Test
    void stsRoleCorrectConfig() {
        final var props = new HashMap<String, String>();

        props.put(S3SinkConfig.AWS_STS_ROLE_ARN, "arn:aws:iam::12345678910:role/S3SinkTask");
        props.put(S3SinkConfig.AWS_STS_ROLE_EXTERNAL_ID, "EXTERNAL_ID");
        props.put(S3SinkConfig.AWS_STS_ROLE_SESSION_NAME, "SESSION_NAME");
        props.put(S3SinkConfig.AWS_S3_BUCKET_NAME_CONFIG, "THE_BUCKET");
        props.put(S3SinkConfig.AWS_S3_REGION_CONFIG, Regions.US_EAST_1.getName());

        final var conf = new S3SinkConfig(props);

        assertEquals("arn:aws:iam::12345678910:role/S3SinkTask", conf.getStsRole().getArn());
        assertEquals("EXTERNAL_ID", conf.getStsRole().getExternalId());
        assertEquals("SESSION_NAME", conf.getStsRole().getSessionName());
        assertEquals(Regions.US_EAST_1, conf.getAwsS3Region());
    }

    @Test
    void stsRoleEmptyArn() {
        final var props = new HashMap<String, String>();

        props.put(S3SinkConfig.AWS_STS_ROLE_EXTERNAL_ID, "EXTERNAL_ID");
        props.put(S3SinkConfig.AWS_STS_ROLE_SESSION_NAME, "SESSION_NAME");
        props.put(S3SinkConfig.AWS_S3_BUCKET_NAME_CONFIG, "THE_BUCKET");
        props.put(S3SinkConfig.AWS_S3_REGION_CONFIG, Regions.US_EAST_1.getName());

        final Throwable t = assertThrows(
            ConfigException.class,
            () -> new S3SinkConfig(props)
        );
        assertEquals(
                "Either {aws.access.key.id, aws.secret.access.key} or"
                        + " {aws.sts.role.arn, aws.sts.role.session.name} should be set",
                t.getMessage()
        );
    }

    @Test
    void stsRoleEmptySessionName() {
        final var props = new HashMap<String, String>();

        props.put(S3SinkConfig.AWS_STS_ROLE_ARN, "arn:aws:iam::12345678910:role/S3SinkTask");
        props.put(S3SinkConfig.AWS_STS_ROLE_EXTERNAL_ID, "EXTERNAL_ID");
        props.put(S3SinkConfig.AWS_S3_BUCKET_NAME_CONFIG, "THE_BUCKET");
        props.put(S3SinkConfig.AWS_S3_REGION_CONFIG, Regions.US_EAST_1.getName());

        final Throwable t = assertThrows(
            ConfigException.class,
            () -> new S3SinkConfig(props)
        );
        assertEquals(
                "Either {aws.access.key.id, aws.secret.access.key} or"
                        + " {aws.sts.role.arn, aws.sts.role.session.name} should be set",
                t.getMessage()
        );
    }

    @Test
    void stsWrongSessionDuration() {
        final var props = new HashMap<String, String>();

        props.put(S3SinkConfig.AWS_STS_ROLE_ARN, "arn:aws:iam::12345678910:role/S3SinkTask");
        props.put(S3SinkConfig.AWS_STS_ROLE_SESSION_NAME, "SESSION_NAME");
        props.put(S3SinkConfig.AWS_S3_BUCKET_NAME_CONFIG, "THE_BUCKET");
        props.put(S3SinkConfig.AWS_S3_REGION_CONFIG, Regions.US_EAST_1.getName());

        props.put(S3SinkConfig.AWS_STS_ROLE_SESSION_DURATION, "30");

        final Throwable t = assertThrows(
            ConfigException.class,
            () -> new S3SinkConfig(props)
        );
        assertEquals(
                "Invalid value 30 for configuration aws.sts.role.session.duration: Value must be at least 900",
                t.getMessage()
        );

        props.put(S3SinkConfig.AWS_STS_ROLE_SESSION_DURATION, "50000");

        final Throwable t2 = assertThrows(
            ConfigException.class,
            () -> new S3SinkConfig(props)
        );
        assertEquals(
            "Invalid value 50000 for configuration aws.sts.role.session.duration: Value must be no more than 43200",
            t2.getMessage()
        );
    }

    @Test
    void stsCorrectSessionDuration() {
        final var props = new HashMap<String, String>();

        props.put(S3SinkConfig.AWS_STS_ROLE_ARN, "arn:aws:iam::12345678910:role/S3SinkTask");
        props.put(S3SinkConfig.AWS_STS_ROLE_SESSION_NAME, "SESSION_NAME");
        props.put(S3SinkConfig.AWS_S3_BUCKET_NAME_CONFIG, "THE_BUCKET");
        props.put(S3SinkConfig.AWS_S3_REGION_CONFIG, Regions.US_EAST_1.getName());

        props.put(S3SinkConfig.AWS_STS_ROLE_SESSION_DURATION, "900");

        final var conf = new S3SinkConfig(props);

        assertEquals(900, conf.getStsRole().getSessionDurationSeconds());
    }

    @Test
    void stsEndpointShouldNotBeSetWithoutRegion() {
        final var props = new HashMap<String, String>();

        props.put(S3SinkConfig.AWS_STS_ROLE_ARN, "arn:aws:iam::12345678910:role/S3SinkTask");
        props.put(S3SinkConfig.AWS_STS_ROLE_EXTERNAL_ID, "EXTERNAL_ID");
        props.put(S3SinkConfig.AWS_STS_ROLE_SESSION_NAME, "SESSION_NAME");
        props.put(S3SinkConfig.AWS_S3_BUCKET_NAME_CONFIG, "THE_BUCKET");
        props.put(S3SinkConfig.AWS_STS_CONFIG_ENDPOINT, "https://sts.eu-north-1.amazonaws.com");

        final Throwable t3 = assertThrows(
            ConfigException.class,
            () -> new S3SinkConfig(props)
        );
        assertEquals(
            "aws.s3.region should be specified together with aws.sts.config.endpoint",
            t3.getMessage()
        );

    }
}
