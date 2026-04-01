function fmtPrice(v) {
  if (!v) return '—'
  if (v < 0.001) return '$' + v.toExponential(2)
  if (v < 1)     return '$' + v.toFixed(4)
  return '$' + v.toLocaleString('en-US', { maximumFractionDigits: 2 })
}

function MoverRow({ rank, id, symbol, price, change_5min, positive }) {
  return (
    <div className="flex items-center justify-between py-2.5 border-b border-slate-200 last:border-0">
      <div className="flex items-center gap-3">
        <span className="w-4 text-xs text-slate-500">{rank}</span>
        <div>
          <p className="text-sm font-semibold text-slate-900 uppercase">{symbol}</p>
          <p className="text-xs text-slate-500 capitalize">{id}</p>
        </div>
      </div>
      <div className="text-right">
        <p className="text-sm font-semibold text-slate-900">{fmtPrice(price)}</p>
        <p className={`text-xs font-bold ${positive ? 'text-emerald-700' : 'text-rose-700'}`}>
          {positive ? '+' : ''}{change_5min?.toFixed(2)}%
        </p>
      </div>
    </div>
  )
}

function Section({ title, data, positive }) {
  const color = positive ? 'text-emerald-700' : 'text-rose-700'
  const icon  = positive ? '▲' : '▼'
  return (
    <div>
      <h3 className={`text-sm font-semibold mb-3 ${color} flex items-center gap-1`}>
        {icon} {title}
      </h3>
      {!data ? (
        <div className="space-y-2">
          {Array(5).fill(0).map((_, i) => <div key={i} className="h-10 bg-slate-700 rounded animate-pulse" />)}
        </div>
      ) : data.length === 0 ? (
        <p className="text-slate-500 text-xs">Waiting for data…</p>
      ) : (
        data.map(r => <MoverRow key={r.id} {...r} positive={positive} />)
      )}
    </div>
  )
}

export default function TopMovers({ gainers, losers }) {
  return (
    <div className="panel h-full p-5">
      <h2 className="title-serif mb-4 text-2xl text-slate-900">Top Movers <span className="font-normal text-sm text-slate-500">(5 min)</span></h2>
      <div className="space-y-5">
        <Section title="Gainers" data={gainers} positive={true} />
        <div className="border-t border-slate-200" />
        <Section title="Losers"  data={losers}  positive={false} />
      </div>
    </div>
  )
}
