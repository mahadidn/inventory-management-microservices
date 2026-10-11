package com.mdn.management.order.purchaseorder

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.GeneratedValue
import jakarta.persistence.GenerationType
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.time.Instant

@Entity
@Table(name = "purchase_order")
class PurchaseOrder (

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    val id: Long? = null,

    @Column(name = "product_code", nullable = false, length = 50)
    val productCode: String,

    @Column(name = "product_name", nullable = false)
    val productName: String,

    @Column(name = "quantity", nullable = false)
    val quantity: Int,

    @Column(name = "status", nullable = false, length = 20)
    val status: String,

    @Column(name = "created_at", nullable = false)
    val createdAt: Instant = Instant.now()

)