function fmtTime(ts) {
  return new Date(ts).toLocaleString([], {
    month: 'short', day: 'numeric',
    hour: '2-digit', minute: '2-digit',
  })
}

function fmtPrice(v) {
  if (!v) return '—'
  if (v < 0.001) return '$' + v.toExponential(2)
  if (v < 1)     return '$' + v.toFixed(4)
  return '$' + v.toLocaleString('en-US', { maximumFractionDigits: 2 })
}

export default function AlertsFeed({ alerts }) {
  return (
    <div className="panel p-5">
      <h2 className="title-serif mb-4 text-2xl text-slate-900">
        Price Alerts
        <span className="ml-2 text-xs font-normal text-slate-500">&gt;2% move in 5 min</span>
      </h2>

      {!alerts ? (
        <div className="space-y-2">
          {Array(4).fill(0).map((_, i) => <div key={i} className="h-12 rounded bg-slate-200 animate-pulse" />)}
        </div>
      ) : alerts.length === 0 ? (
        <div className="flex flex-col items-center justify-center py-10 text-slate-500">
          <span className="text-3xl mb-2">🔕</span>
          <p className="text-sm">No alerts fired yet</p>
          <p className="text-xs mt-1">Will trigger on &gt;2% moves</p>
        </div>
      ) : (
        <div className="overflow-auto max-h-64">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-xs text-slate-500 border-b border-slate-200">
                <th className="text-left pb-2">Time</th>
                <th className="text-left pb-2">Coin</th>
                <th className="text-right pb-2">Price</th>
                <th className="text-right pb-2">5m Change</th>
                <th className="text-right pb-2">Type</th>
              </tr>
            </thead>
            <tbody>
              {alerts.map((a, i) => (
                <tr key={i} className="border-b border-slate-200/80 transition-colors hover:bg-slate-50">
                  <td className="py-2.5 text-xs text-slate-500">{fmtTime(a.alerted_at)}</td>
                  <td className="py-2.5">
                    <span className="font-semibold text-slate-900 uppercase">{a.symbol}</span>
                    <span className="ml-1 text-xs text-slate-500">({a.coin_id})</span>
                  </td>
                  <td className="py-2.5 text-right text-slate-900">{fmtPrice(a.price)}</td>
                  <td className={`py-2.5 text-right font-bold ${a.change_5min >= 0 ? 'text-emerald-700' : 'text-rose-700'}`}>
                    {a.change_5min >= 0 ? '+' : ''}{a.change_5min?.toFixed(2)}%
                  </td>
                  <td className="py-2.5 text-right">
                    <span className={`text-xs font-bold px-2 py-0.5 rounded-full ${
                      a.alert_type === 'PUMP'
                        ? 'bg-emerald-100 text-emerald-700'
                        : 'bg-rose-100 text-rose-700'
                    }`}>
                      {a.alert_type}
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
