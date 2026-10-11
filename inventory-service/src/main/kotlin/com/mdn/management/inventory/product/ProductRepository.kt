package com.mdn.management.inventory.product

import org.springframework.data.jpa.repository.JpaRepository

interface ProductRepository : JpaRepository<Product, Long> {

    fun findByCode(code: String): Product?

}