package com.tripleauth.hermetix.pnl

import com.tripleauth.hermetix.broker.BrokerClient
import com.tripleauth.hermetix.client.dto.Holding
import java.math.BigDecimal
import java.math.RoundingMode
import java.time.ZonedDateTime

/**
 * 계좌 수익률 리포트.
 *
 * - 평가손익(unrealized)은 보유 종목의 API 값을 그대로 합산한다
 * - 총수익률(totalReturnRate)은 `hermetix.pnl.initial-capital` 설정이 있을 때만 계산된다
 *   (모의투자 시작 자금을 서버가 알려주지 않으므로 사용자가 지정)
 * - 실현손익은 포함하지 않는다 — 체결 내역(fills) 기반 계산은 추후 지원
 */
data class PnlReport(
    val timestamp: ZonedDateTime,
    val accountId: String,
    val currency: String,
    val cash: BigDecimal,
    val portfolioValue: BigDecimal,
    val totalMarketValue: BigDecimal,
    val totalUnrealizedPnl: BigDecimal,
    val totalReturnRate: BigDecimal?,
    val holdings: List<HoldingPnl>,
)

data class HoldingPnl(
    val symbol: String,
    val quantity: BigDecimal,
    val avgEntryPrice: BigDecimal,
    val currentPrice: BigDecimal?,
    val marketValue: BigDecimal?,
    val unrealizedPnl: BigDecimal?,
    val unrealizedPnlRate: BigDecimal?,
    /** 종목 거래 통화 (평가손익 단위). null 이면 계좌 통화 */
    val currency: String? = null,
)

class PnlService(
    private val brokerClient: BrokerClient,
    private val initialCapital: BigDecimal?,
) {

    fun report(): PnlReport {
        val account = brokerClient.getAccount()
        val holdings = brokerClient.getHoldings().holdings

        val totalMarketValue = holdings.sumOfOrZero { it.marketValue }
        // 평가손익 합계는 계좌 통화 종목만 — 외화 종목 손익은 단위가 달라 각 holding 에만 있다
        val totalUnrealizedPnl = holdings
            .filter { it.currency == null || it.currency == account.currency }
            .sumOfOrZero { it.unrealizedPnl }

        val totalReturnRate = initialCapital
            ?.takeIf { it > BigDecimal.ZERO }
            ?.let { (account.portfolioValue - it).divide(it, 6, RoundingMode.HALF_EVEN) }

        return PnlReport(
            timestamp = ZonedDateTime.now(),
            accountId = account.accountId,
            currency = account.currency,
            cash = account.cash,
            portfolioValue = account.portfolioValue,
            totalMarketValue = totalMarketValue,
            totalUnrealizedPnl = totalUnrealizedPnl,
            totalReturnRate = totalReturnRate,
            holdings = holdings.map {
                HoldingPnl(
                    symbol = it.symbol,
                    quantity = it.quantity,
                    avgEntryPrice = it.avgEntryPrice,
                    currentPrice = it.currentPrice,
                    marketValue = it.marketValue,
                    unrealizedPnl = it.unrealizedPnl,
                    unrealizedPnlRate = it.unrealizedPnlRate,
                    currency = it.currency,
                )
            },
        )
    }

    private fun List<Holding>.sumOfOrZero(selector: (Holding) -> BigDecimal?): BigDecimal =
        fold(BigDecimal.ZERO) { acc, holding -> acc + (selector(holding) ?: BigDecimal.ZERO) }
}
