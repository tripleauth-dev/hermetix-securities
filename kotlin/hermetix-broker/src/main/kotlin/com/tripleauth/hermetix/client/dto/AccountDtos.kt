package com.tripleauth.hermetix.client.dto

import java.math.BigDecimal

data class AccountResponse(
    val accountId: String,
    val name: String?,
    val currency: String,
    val cash: BigDecimal,
    val portfolioValue: BigDecimal,
    val status: String,
)

data class HoldingsResponse(
    val holdings: List<Holding>,
    val summary: HoldingsSummary? = null,
)

data class Holding(
    val symbol: String,
    val quantity: BigDecimal,
    val avgEntryPrice: BigDecimal,
    val currentPrice: BigDecimal?,
    val marketValue: BigDecimal?,
    val unrealizedPnl: BigDecimal?,
    val unrealizedPnlRate: BigDecimal?,
    /** 종목 거래 통화 — avgEntryPrice·currentPrice·unrealizedPnl 의 단위. null 이면 계좌 통화. marketValue 는 항상 계좌 통화 */
    val currency: String? = null,
)

data class HoldingsSummary(
    val totalMarketValue: BigDecimal?,
    val totalUnrealizedPnl: BigDecimal?,
)

data class BuyingPowerResponse(
    val accountId: String,
    val currency: String,
    val buyingPower: BigDecimal,
)
