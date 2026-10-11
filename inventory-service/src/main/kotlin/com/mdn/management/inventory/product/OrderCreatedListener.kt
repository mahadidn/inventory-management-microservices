package com.mdn.management.inventory.product

import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.handler.annotation.Header
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional

@Component
class OrderCreatedListener(
    private val productRepository: ProductRepository
) {
    companion object {
        const val ORDER_CREATED_TOPIC = "order-created"
    }
    private val logger = LoggerFactory.getLogger(OrderCreatedListener::class.java)

    /**
     * @KafkaListener(topics = ["order-created"], groupId = "inventory-service")
     * Ini satu-satunya anotasi yang dibutuhkan untuk menjadi consumer. Spring Kafka akan membuat
     * consumer di belakang layar, terus-menerus mengambil pesan dari topic order-created,
     * mengubahnya menjadi OrderCreatedEvent lewat deserializer tadi, lalu memanggil method ini
     * sekali untuk setiap pesan.
     */
    @KafkaListener(
        topics = [ORDER_CREATED_TOPIC],
        groupId = "inventory-service"
    )
    @Transactional
    fun onOrderCreated(
        event: OrderCreatedEvent,
        @Header(KafkaHeaders.RECEIVED_PARTITION) partition: Int,
        @Header(KafkaHeaders.OFFSET) offset: Long,
    ) {
        logger.info(
            "Received OrderCreatedEvent for order {} (product {}, quantity {}) from partition {} offset {}",
            event.orderId, event.productCode, event.quantity, partition, offset
        )

        val product = productRepository.findByCode(event.productCode)

        if(product == null) {
            logger.warn("Product {} not found, skipping order {}", event.productCode, event.orderId)
            return
        }

        if(product.stock < event.quantity) {
            logger.warn(
                "Stock for {} is {} but order {} needs {}, skipping",
                product.code, product.stock, event.orderId, event.quantity
            )
            return
        }

        val stockBefore = product.stock
        product.stock -= event.quantity
        logger.info(
            "Stock for {} decreased from {} to {} because of order {}",
            product.code, stockBefore, product.stock, event.orderId
        )
    }

}