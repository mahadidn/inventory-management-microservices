package com.mdn.management.inventory.product

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.GeneratedValue
import jakarta.persistence.GenerationType
import jakarta.persistence.Id
import jakarta.persistence.Table

@Entity
@Table(name =  "product")
class Product (

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    var id: Long? = null,

    @Column(name = "code", nullable = false, unique = true, length = 50)
    val code: String,

    @Column(name = "name", nullable = false)
    val name: String,

    @Column(name = "stock", nullable = false)
    var stock: Int,
)