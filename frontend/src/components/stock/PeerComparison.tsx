import { useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import { Users, Loader2 } from "lucide-react";
import { clsx } from "clsx";
import { fetchStockPeers, type PeerStock } from "../../api/client.ts";
import { formatNumber, formatPct, formatCompact, colorBySign } from "../../lib/format.ts";

interface PeerComparisonProps {
  symbol: string;
}

export default function PeerComparison({ symbol }: PeerComparisonProps) {
  const navigate = useNavigate();
  const [sector, setSector] = useState<string | null>(null);
  const [peers, setPeers] = useState<PeerStock[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);

    fetchStockPeers(symbol)
      .then((data) => {
        if (cancelled) return;
        setSector(data.sector);
        setPeers(data.peers);
      })
      .catch(() => {
        if (cancelled) return;
        setPeers([]);
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });

    return () => {
      cancelled = true;
    };
  }, [symbol]);

  if (loading) {
    return (
      <div className="bg-[var(--surface)] border border-[var(--border)] rounded-lg p-5">
        <div className="flex items-center gap-2 text-xs text-[var(--text-muted)]">
          <Loader2 className="h-3.5 w-3.5 animate-spin" />
          Loading peers...
        </div>
      </div>
    );
  }

  if (peers.length === 0) {
    return null;
  }

  return (
    <div className="bg-[var(--surface)] border border-[var(--border)] rounded-lg p-5">
      <h2 className="text-xs font-semibold text-[var(--text-muted)] uppercase tracking-wider mb-1 flex items-center gap-2">
        <Users className="h-3.5 w-3.5 text-blue-500" />
        Peer Comparison
      </h2>
      {sector && (
        <p className="text-[10px] text-[var(--text-dim)] mb-3">
          Sector: {sector}
        </p>
      )}

      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead>
            <tr className="text-[10px] text-[var(--text-dim)] uppercase tracking-wider border-b border-[var(--border)]">
              <th className="text-left py-1.5 pr-2">Symbol</th>
              <th className="text-right py-1.5 px-2">LTP</th>
              <th className="text-right py-1.5 px-2">Change%</th>
              <th className="text-right py-1.5 px-2">Volume</th>
              <th className="text-right py-1.5 pl-2">Turnover</th>
            </tr>
          </thead>
          <tbody>
            {peers.map((peer) => (
              <tr
                key={peer.symbol}
                onClick={() => navigate(`/stock/${peer.symbol}`)}
                className="border-b border-[var(--border)] last:border-0 hover:bg-[var(--hover)] cursor-pointer transition-colors"
              >
                <td className="py-1.5 pr-2 font-medium text-[var(--text)]">
                  {peer.symbol}
                </td>
                <td className="py-1.5 px-2 text-right text-[var(--text)] tabular-nums">
                  {formatNumber(peer.ltp)}
                </td>
                <td
                  className={clsx(
                    "py-1.5 px-2 text-right font-medium tabular-nums",
                    colorBySign(peer.change_pct),
                  )}
                >
                  {formatPct(peer.change_pct)}
                </td>
                <td className="py-1.5 px-2 text-right text-[var(--text-muted)] tabular-nums">
                  {formatCompact(peer.volume)}
                </td>
                <td className="py-1.5 pl-2 text-right text-[var(--text-muted)] tabular-nums">
                  {formatCompact(peer.value)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
