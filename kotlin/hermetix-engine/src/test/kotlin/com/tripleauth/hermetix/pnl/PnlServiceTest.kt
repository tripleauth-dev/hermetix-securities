package com.tripleauth.hermetix.pnl

import com.tripleauth.hermetix.Fixtures
import com.tripleauth.hermetix.broker.BrokerClient
import com.tripleauth.hermetix.client.dto.Holding
import com.tripleauth.hermetix.client.dto.HoldingsResponse
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.math.BigDecimal

class PnlServiceTest {

    private val brokerClient = mockk<BrokerClient>()

    private fun holding(symbol: String, marketValue: String?, pnl: String?, currency: String? = null) = Holding(
        symbol = symbol,
        quantity = BigDecimal.ONE,
        avgEntryPrice = BigDecimal("100"),
        currentPrice = null,
        marketValue = marketValue?.let { BigDecimal(it) },
        unrealizedPnl = pnl?.let { BigDecimal(it) },
        unrealizedPnlRate = null,
        currency = currency,
    )

    @Test
    fun `보유 평가액과 평가손익을 합산한다`() {
        every { brokerClient.getAccount() } returns Fixtures.account() // portfolioValue=20000
        every { brokerClient.getHoldings() } returns HoldingsResponse(
            holdings = listOf(
                holding("AAPL", "600", "50"),
                holding("MSFT", "900", "-20"),
                holding("NVDA", null, null), // null 필드는 0으로 취급
            ),
        )

        val report = PnlService(brokerClient, initialCapital = null).report()

        assertThat(report.totalMarketValue).isEqualByComparingTo(BigDecimal("1500"))
        assertThat(report.totalUnrealizedPnl).isEqualByComparingTo(BigDecimal("30"))
        assertThat(report.totalReturnRate).isNull()
        assertThat(report.holdings).hasSize(3)
    }

    @Test
    fun `외화 종목 손익은 합계에서 빼고 평가액은 더한다`() {
        every { brokerClient.getAccount() } returns Fixtures.account().copy(currency = "KRW")
        every { brokerClient.getHoldings() } returns HoldingsResponse(
            holdings = listOf(
                holding("KRX:005930", "210000", "6000", currency = "KRW"),
                holding("US:AAPL", "700000", "100", currency = "USD"), // 손익 100 USD — 원화 합계에 섞지 않는다
            ),
        )

        val report = PnlService(brokerClient, initialCapital = null).report()

        assertThat(report.totalMarketValue).isEqualByComparingTo(BigDecimal("910000"))
        assertThat(report.totalUnrealizedPnl).isEqualByComparingTo(BigDecimal("6000"))
        assertThat(report.holdings.map { it.currency }).containsExactly("KRW", "USD")
    }

    @Test
    fun `시작 자금이 설정되면 총수익률을 계산한다`() {
        every { brokerClient.getAccount() } returns Fixtures.account() // portfolioValue=20000
        every { brokerClient.getHoldings() } returns HoldingsResponse(holdings = emptyList())

        val report = PnlService(brokerClient, initialCapital = BigDecimal("18000")).report()

        // (20000 - 18000) / 18000 = 0.111111
        assertThat(report.totalReturnRate).isEqualByComparingTo(BigDecimal("0.111111"))
    }
}
