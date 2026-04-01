function fmtTime(ts) {
  if (!ts) return '—'
  return new Date(ts).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' })
}

function StatusDot({ status }) {
  const ok = status === 'success'
  return (
    <span className={`inline-block w-2 h-2 rounded-full mr-2 ${ok ? 'bg-emerald-600' : 'bg-rose-600'}`} />
  )
}

export default function PipelineStatus({ pipeline }) {
  const allOk = pipeline?.every(t => t.status === 'success')

  return (
    <div className="panel p-5">
      <div className="flex items-center justify-between mb-4">
        <h2 className="title-serif text-2xl text-slate-900">Pipeline Health</h2>
        {pipeline && (
          <span className={`text-xs font-semibold px-2 py-0.5 rounded-full ${
            allOk ? 'bg-emerald-100 text-emerald-700' : 'bg-rose-100 text-rose-700'
          }`}>
            {allOk ? '● All Systems Go' : '● Degraded'}
          </span>
        )}
      </div>

      {!pipeline ? (
        <div className="space-y-3">
          {Array(4).fill(0).map((_, i) => <div key={i} className="h-14 rounded bg-slate-200 animate-pulse" />)}
        </div>
      ) : pipeline.length === 0 ? (
        <p className="text-slate-500 text-sm">No pipeline data yet</p>
      ) : (
        <div className="space-y-3">
          {pipeline.map((task, i) => (
            <div key={i} className="rounded-lg border border-slate-200 bg-white/70 p-3">
              <div className="flex items-center justify-between mb-1">
                <div className="flex items-center">
                  <StatusDot status={task.status} />
                  <span className="text-sm font-medium text-slate-900">{task.task}</span>
                </div>
                <span className={`text-xs px-2 py-0.5 rounded font-semibold ${
                  task.status === 'success'
                    ? 'bg-emerald-100 text-emerald-700'
                    : 'bg-rose-100 text-rose-700'
                }`}>
                  {task.status}
                </span>
              </div>
              <div className="mt-1 flex gap-4 pl-4 text-xs text-slate-500">
                <span>Records: <span className="text-slate-800">{task.records_pushed ?? '—'}</span></span>
                <span>Latency: <span className="text-slate-800">{task.latency_ms ? `${task.latency_ms}ms` : '—'}</span></span>
                <span>At: <span className="text-slate-800">{fmtTime(task.created_at)}</span></span>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
