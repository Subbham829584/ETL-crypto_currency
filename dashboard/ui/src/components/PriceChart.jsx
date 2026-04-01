import { useState, useEffect } from 'react'
import axios from 'axios'
import {
  ResponsiveContainer, LineChart, Line, XAxis, YAxis,
  CartesianGrid, Tooltip, Legend,
} from 'recharts'

const WINDOWS = [
  { label: '30m', value: 30 },
  { label: '1h',  value: 60 },
  { label: '3h',  value: 180 },
  { label: '6h',  value: 360 },
]

function fmtTime(ts) {
  return new Date(ts).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
}

function fmtPrice(v) {
  if (!v) return ''
  if (v < 0.001) return v.toExponential(2)
  if (v < 1)     return v.toFixed(4)
  if (v < 1000)  return v.toFixed(2)
  return v.toLocaleString('en-US', { maximumFractionDigits: 0 })
}

const CustomTooltip = ({ active, payload, label }) => {
  if (!active || !payload?.length) return null
  return (
    <div className="rounded-lg border border-slate-200 bg-white/95 p-3 text-xs shadow-xl">
      <p className="mb-2 text-slate-500">{fmtTime(label)}</p>
      {payload.map(p => (
        <div key={p.dataKey} className="flex justify-between gap-4">
          <span style={{ color: p.color }}>{p.name}</span>
          <span className="font-bold text-slate-900">${fmtPrice(p.value)}</span>
        </div>
      ))}
    </div>
  )
}

export default function PriceChart({ coinId }) {
  const [data, setData]       = useState([])
  const [window, setWindow]   = useState(60)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    setLoading(true)
    axios.get(`/api/coins/${coinId}/history?minutes=${window}`)
      .then(r => setData(r.data))
      .finally(() => setLoading(false))
  }, [coinId, window])

  return (
    <div className="panel p-5">
      <div className="flex items-center justify-between mb-5">
        <div>
          <h2 className="title-serif text-2xl capitalize text-slate-900">{coinId}</h2>
          <p className="text-xs text-slate-600">Price · SMA · EMA</p>
        </div>
        <div className="flex gap-1">
          {WINDOWS.map(w => (
            <button
              key={w.value}
              onClick={() => setWindow(w.value)}
              className={`text-xs px-3 py-1 rounded-md font-medium transition-all ${
                window === w.value
                  ? 'bg-sky-100 text-sky-700 border border-sky-200'
                  : 'text-slate-600 hover:text-slate-900 hover:bg-slate-100'
              }`}
            >
              {w.label}
            </button>
          ))}
        </div>
      </div>

      {loading ? (
        <div className="h-64 flex items-center justify-center text-slate-500 text-sm">Loading…</div>
      ) : data.length === 0 ? (
        <div className="h-64 flex items-center justify-center text-slate-500 text-sm">No data yet</div>
      ) : (
        <ResponsiveContainer width="100%" height={280}>
          <LineChart data={data} margin={{ top: 5, right: 10, bottom: 5, left: 10 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#dbe5f2" />
            <XAxis
              dataKey="timestamp"
              tickFormatter={fmtTime}
              tick={{ fill: '#64748b', fontSize: 11 }}
              axisLine={false}
              tickLine={false}
              interval="preserveStartEnd"
            />
            <YAxis
              tickFormatter={fmtPrice}
              tick={{ fill: '#64748b', fontSize: 11 }}
              axisLine={false}
              tickLine={false}
              width={70}
              domain={['auto', 'auto']}
            />
            <Tooltip content={<CustomTooltip />} />
            <Legend
              wrapperStyle={{ fontSize: 12, color: '#475569' }}
            />
            <Line
              type="monotone" dataKey="price" name="Price"
              stroke="#2e6fdb" strokeWidth={2.25} dot={false} activeDot={{ r: 4 }}
            />
            <Line
              type="monotone" dataKey="sma" name="SMA"
              stroke="#d8903f" strokeWidth={1.6} dot={false} strokeDasharray="4 2"
            />
            <Line
              type="monotone" dataKey="ema" name="EMA"
              stroke="#157957" strokeWidth={1.6} dot={false} strokeDasharray="4 2"
            />
          </LineChart>
        </ResponsiveContainer>
      )}
    </div>
  )
}
