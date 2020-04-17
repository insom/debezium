/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import static org.fest.assertions.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

/**
 * @author Aaron Brady
 */
public class RewriteNamespaceTest {

    @Test
    public void testKeyReplacementWorkingConfiguration() {
        final RewriteNamespace<SourceRecord> router = new RewriteNamespace<>();
        final Map<String, String> props = new HashMap<>();

        props.put("topic.regex", "(.*).shopify_shard_\\d+.(.*)");
        props.put("topic.replacement", "shopify_core.$2");
        router.configure(props);

        Schema keySchema = SchemaBuilder.struct()
                .name("s21.shopify_shard_21.address.Key")
                .field("id", SchemaBuilder.int64().build())
                .build();

        Struct key1 = new Struct(keySchema).put("id", 123L);

        SourceRecord record1 = new SourceRecord(
                new HashMap<>(), new HashMap<>(), "s21.shopify_shard_21.address", keySchema, key1, null, null);

        SourceRecord transformed1 = router.apply(record1);
        assertThat(transformed1).isNotNull();
        assertThat(transformed1.topic()).isEqualTo("s21.shopify_shard_21.address"); // no change expected

        assertThat(transformed1.keySchema().name()).isEqualTo("shopify_core.address.Key");
        assertThat(transformed1.keySchema().fields()).hasSize(1);
        assertThat(transformed1.keySchema().fields().get(0).name()).isEqualTo("id");

        assertThat(((Struct) transformed1.key()).get("id")).isEqualTo(123L);

        Struct key2 = new Struct(keySchema).put("id", 123L);

        // This second record is basically the same as the first, and should _actually_ be indistinguishable
        keySchema = SchemaBuilder.struct()
                .name("s22.shopify_shard_22.address.Key")
                .field("id", SchemaBuilder.int64().build())
                .build();
        SourceRecord record2 = new SourceRecord(
                new HashMap<>(), new HashMap<>(), "s22.shopify_shard_22.address", keySchema, key2, null, null);

        SourceRecord transformed2 = router.apply(record2);
        assertThat(transformed2).isNotNull();
        assertThat(transformed2.topic()).isEqualTo("s22.shopify_shard_22.address");

        assertThat(transformed2.keySchema().name()).isEqualTo("shopify_core.address.Key");
        assertThat(transformed2.keySchema().fields()).hasSize(1);
        assertThat(transformed2.keySchema().fields().get(0).name()).isEqualTo("id");

        assertThat(((Struct) transformed2.key()).get("id")).isEqualTo(123L);
    }
}
