package com.mdn.management.order.purchaseorder

import org.springframework.data.jpa.repository.JpaRepository

interface PurchaseOrderRepository : JpaRepository<PurchaseOrder, Long>