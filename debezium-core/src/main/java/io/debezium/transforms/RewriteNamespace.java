/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.data.Envelope;
import io.debezium.util.SchemaNameAdjuster;

/**
 * A logical table consists of one or more physical tables with the same schema. A common use case is sharding -- the
 * two physical tables `db_shard1.my_table` and `db_shard2.my_table` together form one logical table.
 *
 * @param <R> the subtype of {@link ConnectRecord} on which this transformation will operate
 * @author David Leibovic
 * @author Mario Mueller
 * @author Aaron Brady
 */
public class RewriteNamespace<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Field TOPIC_REGEX = Field.create("topic.regex")
            .withDisplayName("Topic regex")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.LOW)
            .withValidation(Field::isRequired, Field::isRegex)
            .withDescription("The regex used for extracting the name of the logical table from the original topic name.");
    private static final Field TOPIC_REPLACEMENT = Field.create("topic.replacement")
            .withDisplayName("Topic replacement")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.LOW)
            .withValidation(Field::isRequired)
            .withDescription("The replacement string used in conjunction with " + TOPIC_REGEX.name() +
                    ". This will be used to create the new topic name.");

    private static final Logger logger = LoggerFactory.getLogger(RewriteNamespace.class);
    private final SchemaNameAdjuster schemaNameAdjuster = SchemaNameAdjuster.create(logger);
    private Pattern topicRegex;
    private String topicReplacement;
    private final Cache<Schema, Schema> keySchemaUpdateCache = new SynchronizedCache<>(new LRUCache<Schema, Schema>(16));
    private final Cache<Schema, Schema> envelopeSchemaUpdateCache = new SynchronizedCache<>(new LRUCache<Schema, Schema>(16));
    private final Cache<String, String> topicRegexReplaceCache = new SynchronizedCache<>(new LRUCache<String, String>(16));

    @Override
    public void configure(Map<String, ?> props) {
        Configuration config = Configuration.from(props);
        final Field.Set configFields = Field.setOf(
                TOPIC_REGEX,
                TOPIC_REPLACEMENT);

        if (!config.validateAndRecord(configFields, logger::error)) {
            throw new ConnectException("Unable to validate config.");
        }

        topicRegex = Pattern.compile(config.getString(TOPIC_REGEX));
        topicReplacement = config.getString(TOPIC_REPLACEMENT);
    }

    @Override
    public R apply(R record) {
        final String oldNamespace = record.topic();
        final String newNamespace = determineNewNamespace(oldNamespace);

        if (newNamespace == null) {
            return record;
        }

        logger.debug("Applying topic name transformation from {} to {}", oldNamespace, newNamespace);

        Schema newKeySchema = null;
        Struct newKey = null;

        // Key could be null in the case of a table without a primary key
        if (record.key() != null) {
            final Struct oldKey = requireStruct(record.key(), "Updating schema");
            newKeySchema = updateKeySchema(oldKey.schema(), newNamespace);
            newKey = updateKey(newKeySchema, oldKey, oldNamespace);
        }

        if (record.value() == null) {
            // Value will be null in the case of a delete event tombstone
            return record.newRecord(
                    record.topic(),
                    record.kafkaPartition(),
                    newKeySchema,
                    newKey,
                    record.valueSchema(),
                    record.value(),
                    record.timestamp());
        }

        final Struct oldEnvelope = requireStruct(record.value(), "Updating schema");
        final Schema newEnvelopeSchema = updateEnvelopeSchema(oldEnvelope.schema(), newNamespace);
        final Struct newEnvelope = updateEnvelope(newEnvelopeSchema, oldEnvelope);

        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                newKeySchema,
                newKey,
                newEnvelopeSchema,
                newEnvelope,
                record.timestamp());
    }

    @Override
    public void close() {
    }

    @Override
    public ConfigDef config() {
        ConfigDef config = new ConfigDef();
        Field.group(
                config,
                null,
                TOPIC_REGEX,
                TOPIC_REPLACEMENT);
        return config;
    }

    /**
     * Determine the new namespace.
     *
     * @param oldNamespace the name of the old namespace
     * @return return the new namespace, if the regex applies. Otherwise, return null.
     */
    private String determineNewNamespace(String oldNamespace) {
        String newNamespace = topicRegexReplaceCache.get(oldNamespace);
        if (newNamespace != null) {
            return newNamespace;
        }
        else {
            final Matcher matcher = topicRegex.matcher(oldNamespace);
            if (matcher.matches()) {
                newNamespace = matcher.replaceFirst(topicReplacement);
                topicRegexReplaceCache.put(oldNamespace, newNamespace);
                return newNamespace;
            }
            return null;
        }
    }

    private Schema updateKeySchema(Schema oldKeySchema, String newNamespaceName) {
        Schema newKeySchema = keySchemaUpdateCache.get(oldKeySchema);
        if (newKeySchema != null) {
            return newKeySchema;
        }

        final SchemaBuilder builder = copySchemaExcludingName(oldKeySchema, SchemaBuilder.struct());
        builder.name(schemaNameAdjuster.adjust(newNamespaceName + ".Key"));

        newKeySchema = builder.build();
        keySchemaUpdateCache.put(oldKeySchema, newKeySchema);
        return newKeySchema;
    }

    private Struct updateKey(Schema newKeySchema, Struct oldKey, String oldNamespace) {
        final Struct newKey = new Struct(newKeySchema);
        for (org.apache.kafka.connect.data.Field field : oldKey.schema().fields()) {
            newKey.put(field.name(), oldKey.get(field));
        }
        return newKey;
    }

    private Schema updateEnvelopeSchema(Schema oldEnvelopeSchema, String newNamespaceName) {
        Schema newEnvelopeSchema = envelopeSchemaUpdateCache.get(oldEnvelopeSchema);
        if (newEnvelopeSchema != null) {
            return newEnvelopeSchema;
        }

        final Schema oldValueSchema = oldEnvelopeSchema.field(Envelope.FieldName.BEFORE).schema();
        final SchemaBuilder valueBuilder = copySchemaExcludingName(oldValueSchema, SchemaBuilder.struct());
        valueBuilder.name(schemaNameAdjuster.adjust(newNamespaceName + ".Value"));
        final Schema newValueSchema = valueBuilder.build();

        final SchemaBuilder envelopeBuilder = copySchemaExcludingName(oldEnvelopeSchema, SchemaBuilder.struct(), false);
        for (org.apache.kafka.connect.data.Field field : oldEnvelopeSchema.fields()) {
            final String fieldName = field.name();
            Schema fieldSchema = field.schema();
            if (Objects.equals(fieldName, Envelope.FieldName.BEFORE) || Objects.equals(fieldName, Envelope.FieldName.AFTER)) {
                fieldSchema = newValueSchema;
            }
            envelopeBuilder.field(fieldName, fieldSchema);
        }
        envelopeBuilder.name(schemaNameAdjuster.adjust(Envelope.schemaName(newNamespaceName)));

        newEnvelopeSchema = envelopeBuilder.build();
        envelopeSchemaUpdateCache.put(oldEnvelopeSchema, newEnvelopeSchema);
        return newEnvelopeSchema;
    }

    private Struct updateEnvelope(Schema newEnvelopeSchema, Struct oldEnvelope) {
        final Struct newEnvelope = new Struct(newEnvelopeSchema);
        final Schema newValueSchema = newEnvelopeSchema.field(Envelope.FieldName.BEFORE).schema();
        for (org.apache.kafka.connect.data.Field field : oldEnvelope.schema().fields()) {
            final String fieldName = field.name();
            Object fieldValue = oldEnvelope.get(field);
            if ((Objects.equals(fieldName, Envelope.FieldName.BEFORE) || Objects.equals(fieldName, Envelope.FieldName.AFTER))
                    && fieldValue != null) {
                fieldValue = updateValue(newValueSchema, requireStruct(fieldValue, "Updating schema"));
            }
            newEnvelope.put(fieldName, fieldValue);
        }

        return newEnvelope;
    }

    private Struct updateValue(Schema newValueSchema, Struct oldValue) {
        final Struct newValue = new Struct(newValueSchema);
        for (org.apache.kafka.connect.data.Field field : oldValue.schema().fields()) {
            newValue.put(field.name(), oldValue.get(field));
        }
        return newValue;
    }

    private SchemaBuilder copySchemaExcludingName(Schema source, SchemaBuilder builder) {
        return copySchemaExcludingName(source, builder, true);
    }

    private SchemaBuilder copySchemaExcludingName(Schema source, SchemaBuilder builder, boolean copyFields) {
        builder.version(source.version());
        builder.doc(source.doc());

        Map<String, String> params = source.parameters();
        if (params != null) {
            builder.parameters(params);
        }

        if (source.isOptional()) {
            builder.optional();
        }
        else {
            builder.required();
        }

        if (copyFields) {
            for (org.apache.kafka.connect.data.Field field : source.fields()) {
                builder.field(field.name(), field.schema());
            }
        }

        return builder;
    }
}
