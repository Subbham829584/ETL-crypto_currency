import { useState, useEffect, useCallback } from 'react'
import axios from 'axios'
import CoinGrid from './components/CoinGrid'
import PriceChart from './components/PriceChart'
import TopMovers from './components/TopMovers'
import AlertsFeed from './components/AlertsFeed'
import PipelineStatus from './components/PipelineStatus'

const REFRESH_MS = 30_000

function useApi(url, deps = []) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)

  const fetch = useCallback(async () => {
    try {
      const res = await axios.get(url)
      setData(res.data)
    } catch {
      // keep previous value
    } finally {
      setLoading(false)
    }
  }, [url])

  useEffect(() => { fetch() }, [fetch, ...deps])
  useEffect(() => {
    const id = setInterval(fetch, REFRESH_MS)
    return () => clearInterval(id)
  }, [fetch])

  return { data, loading }
}

function formatLag(seconds) {
  if (seconds === null || seconds === undefined) return 'n/a'
  if (seconds < 60) return `${Math.round(seconds)} sec`
  if (seconds < 3600) return `${Math.round(seconds / 60)} min`
  return `${(seconds / 3600).toFixed(1)} hr`
}

function fmtMetric(value, digits = 2) {
  if (value === null || value === undefined) return 'n/a'
  if (typeof value === 'number') return value.toLocaleString('en-US', { maximumFractionDigits: digits })
  return value
}

export default function App() {
  const [selectedCoin, setSelectedCoin] = useState('bitcoin')
  const [lastRefresh, setLastRefresh] = useState(new Date())

  const { data: coins } = useApi('/api/coins/latest')
  const { data: gainers } = useApi('/api/gainers')
  const { data: losers } = useApi('/api/losers')
  const { data: alerts } = useApi('/api/alerts')
  const { data: pipeline } = useApi('/api/pipeline')
  const { data: stats } = useApi('/api/stats')
  const { data: health } = useApi('/api/health')
  const { data: anomalies } = useApi('/api/anomalies?minutes=120&zscore=2.5')
  const { data: summary } = useApi(`/api/coins/${selectedCoin}/summary?minutes=180`, [selectedCoin])

  useEffect(() => {
    const id = setInterval(() => setLastRefresh(new Date()), REFRESH_MS)
    return () => clearInterval(id)
  }, [])

  const pipelineOk = pipeline?.every((t) => t.status === 'success')
  const heroBadgeClass = pipelineOk ? 'chip chip-live' : 'chip chip-risk'

  return (
    <div className="app-shell grain-overlay px-4 py-5 sm:px-6 lg:px-10">
      <header className="panel fade-up relative overflow-hidden p-6 md:p-8">
        <div className="absolute -right-24 -top-20 h-48 w-48 rounded-full bg-[#ffe0b9]/60 blur-2xl" />
        <div className="absolute -left-16 -bottom-16 h-48 w-48 rounded-full bg-[#c7dbff]/60 blur-2xl" />
        <div className="relative z-10">
          <p className="mb-2 text-[11px] font-semibold uppercase tracking-[0.28em] text-sky-700">Market Intelligence Desk</p>
          <div className="flex flex-col gap-5 lg:flex-row lg:items-end lg:justify-between">
            <div>
              <h1 className="title-serif text-4xl leading-tight text-slate-900 md:text-5xl">MarketFlow Atlas</h1>
              <p className="mt-2 max-w-2xl text-sm muted">
                Elegant real-time crypto analytics with operational telemetry from Airflow, Kafka and Spark.
              </p>
            </div>
            <div className="flex flex-wrap items-center gap-2">
              <span className={heroBadgeClass}>{pipelineOk ? 'Pipeline Healthy' : 'Pipeline Degraded'}</span>
              <span className="chip muted">Data Lag: {formatLag(health?.data_lag_seconds)}</span>
              <span className="chip muted">Refresh: {lastRefresh.toLocaleTimeString()}</span>
            </div>
          </div>
          <div className="mt-5 grid grid-cols-2 gap-3 md:grid-cols-5">
            <div className="metric-tile rounded-xl p-3.5">
              <p className="text-[10px] uppercase tracking-[0.18em] muted">Coins</p>
              <p className="mt-1 text-xl font-bold text-slate-900">{coins?.length ?? 0}</p>
            </div>
            <div className="metric-tile rounded-xl p-3.5">
              <p className="text-[10px] uppercase tracking-[0.18em] muted">Last Data</p>
              <p className="mt-1 text-lg font-semibold text-slate-900">
                {stats?.last_updated ? new Date(stats.last_updated).toLocaleTimeString() : 'n/a'}
              </p>
            </div>
            <div className="metric-tile rounded-xl p-3.5">
              <p className="text-[10px] uppercase tracking-[0.18em] muted">3h Net</p>
              <p className={`mt-1 text-lg font-semibold ${summary?.net_change_pct >= 0 ? 'text-emerald-700' : 'text-rose-700'}`}>
                {summary?.net_change_pct === null || summary?.net_change_pct === undefined
                  ? 'n/a'
                  : `${summary.net_change_pct > 0 ? '+' : ''}${summary.net_change_pct}%`}
              </p>
            </div>
            <div className="metric-tile rounded-xl p-3.5">
              <p className="text-[10px] uppercase tracking-[0.18em] muted">Avg Volatility</p>
              <p className="mt-1 text-lg font-semibold text-slate-900">{fmtMetric(summary?.avg_volatility, 4)}</p>
            </div>
            <div className="metric-tile rounded-xl p-3.5">
              <p className="text-[10px] uppercase tracking-[0.18em] muted">Anomalies</p>
              <p className="mt-1 text-lg font-semibold text-slate-900">{anomalies?.length ?? 0}</p>
            </div>
          </div>
        </div>
      </header>

      <main className="mx-auto mt-6 max-w-[1600px] space-y-6">
        <section className="panel p-5">
          <div className="mb-3 flex items-center justify-between">
            <h2 className="title-serif text-2xl text-slate-900">Live Ticker Board</h2>
            <p className="text-xs muted">Tap a coin to focus chart + summary</p>
          </div>
          <CoinGrid coins={coins} selectedCoin={selectedCoin} onSelect={setSelectedCoin} />
        </section>

        <section className="grid grid-cols-1 gap-6 xl:grid-cols-3">
          <div className="xl:col-span-2">
            <PriceChart coinId={selectedCoin} />
          </div>
          <TopMovers gainers={gainers} losers={losers} />
        </section>

        <section className="grid grid-cols-1 gap-6 xl:grid-cols-3">
          <div className="xl:col-span-2">
            <AlertsFeed alerts={alerts} />
          </div>
          <PipelineStatus pipeline={pipeline} />
        </section>
      </main>
    </div>
  )
}
