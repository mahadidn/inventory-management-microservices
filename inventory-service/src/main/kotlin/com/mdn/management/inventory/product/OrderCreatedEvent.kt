package com.mdn.management.inventory.product

data class OrderCreatedEvent(
    val orderId: Long,
    val productCode: String,
    val quantity: Int
)
