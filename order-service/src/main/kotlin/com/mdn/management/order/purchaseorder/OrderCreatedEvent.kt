package com.mdn.management.order.purchaseorder

data class OrderCreatedEvent(
    val orderId: Long,
    val productCode: String,
    val quantity: Int,
)
