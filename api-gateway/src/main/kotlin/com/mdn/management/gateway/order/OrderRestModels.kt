package com.mdn.management.gateway.order

import jakarta.validation.constraints.NotBlank
import jakarta.validation.constraints.Positive

data class CreateOrderRestRequest(
    @NotBlank(message = "productCode is required")
    val productCode: String,

    @Positive(message = "Quantity must be greater than 0")
    val quantity: Int,
)

data class OrderRestResponse(
    val orderId: Long,
    val productCode: String,
    val productName: String,
    val quantity: Int,
    val status: String,
)
