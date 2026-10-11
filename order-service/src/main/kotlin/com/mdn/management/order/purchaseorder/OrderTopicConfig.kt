package com.mdn.management.order.purchaseorder

import org.apache.kafka.clients.admin.NewTopic
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.TopicBuilder

@Configuration
class OrderTopicConfig {

    companion object {
        const val ORDER_CREATED_TOPIC = "order-created"
    }

    /**
     * TopicBuilder adalah helper bawaan Spring Kafka untuk membangun NewTopic dengan gaya builder.
     * a.) .partitions(3): tiga lajur. Kita butuh lebih dari satu partition untuk melihat efek
     *      key sebentar lagi.
     * b.) .replicas(1): setiap partition hanya disimpan di 1 broker, karena kita memang hanya
     *     punya satu Kafka. Di produksi biasanya 3, supaya data tetap aman kalau satu broker mati.
     */
    @Bean
    fun orderCreatedTopic(): NewTopic {
        return TopicBuilder.name(ORDER_CREATED_TOPIC)
            .partitions(3)
            .replicas(1)
            .build()
    }

}