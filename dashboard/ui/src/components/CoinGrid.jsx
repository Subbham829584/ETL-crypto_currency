const fmt = (price) => {
  if (price === null || price === undefined) return 'n/a'
  if (price < 0.001) return price.toExponential(2)
  if (price < 1) return price.toFixed(4)
  if (price < 100) return price.toFixed(2)
  return price.toLocaleString('en-US', { maximumFractionDigits: 2 })
}

function ChangeBadge({ value }) {
  if (value === null || value === undefined) return <span className="text-xs text-slate-400">n/a</span>
  const pos = value >= 0
  return (
    <span className={`text-xs font-semibold ${pos ? 'text-emerald-700' : 'text-rose-700'}`}>
      {pos ? '+' : ''}{value.toFixed(2)}%
    </span>
  )
}

function CoinCard({ coin, selected, onClick }) {
  const change = coin.change_5min
  const glow = selected
    ? 'border-sky-500/70 shadow-[0_10px_24px_rgba(70,120,189,0.22)] -translate-y-0.5'
    : 'border-slate-300/80 hover:border-sky-300'

  return (
    <button
      onClick={onClick}
      className={`w-full rounded-xl border bg-white/70 p-4 text-left transition-all duration-200 hover:-translate-y-0.5 ${glow}`}
    >
      <div className="mb-2 flex items-center justify-between">
        <div>
          <p className="text-[10px] uppercase tracking-[0.15em] text-slate-500">{coin.symbol}</p>
          <p className="max-w-[95px] truncate text-sm font-semibold capitalize text-slate-900">{coin.id}</p>
        </div>
        {change !== null && change !== undefined && (
          <span className={`rounded px-1.5 py-0.5 text-xs font-semibold ${change >= 0 ? 'bg-emerald-100 text-emerald-700' : 'bg-rose-100 text-rose-700'}`}>
            {change >= 0 ? 'UP' : 'DN'}
          </span>
        )}
      </div>
      <p className="text-lg font-semibold text-slate-900">${fmt(coin.price)}</p>
      <div className="mt-1 flex gap-3">
        <div>
          <p className="text-[10px] text-slate-500">1m</p>
          <ChangeBadge value={coin.change_1min} />
        </div>
        <div>
          <p className="text-[10px] text-slate-500">5m</p>
          <ChangeBadge value={coin.change_5min} />
        </div>
      </div>
    </button>
  )
}

export default function CoinGrid({ coins, selectedCoin, onSelect }) {
  if (!coins) {
    return (
      <div className="grid grid-cols-2 gap-3 sm:grid-cols-4 lg:grid-cols-5 xl:grid-cols-10">
        {Array(20).fill(0).map((_, i) => (
          <div key={i} className="h-24 animate-pulse rounded-xl border border-slate-300 bg-white/70 p-4" />
        ))}
      </div>
    )
  }

  return (
    <div className="grid grid-cols-2 gap-3 sm:grid-cols-4 lg:grid-cols-5 xl:grid-cols-10">
      {coins.map((coin) => (
        <CoinCard
          key={coin.id}
          coin={coin}
          selected={selectedCoin === coin.id}
          onClick={() => onSelect(coin.id)}
        />
      ))}
    </div>
  )
}
