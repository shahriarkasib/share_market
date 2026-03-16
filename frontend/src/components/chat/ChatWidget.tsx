import { useState, useRef, useEffect } from "react";
import { Send, Trash2, Loader2, MessageCircle, Bot, User, X, Minus } from "lucide-react";
import { clsx } from "clsx";
import { sendChatMessage, clearChatSession } from "../../api/client.ts";
import type { ChatMessage } from "../../api/client.ts";

const SESSION_KEY = "dse-chat-session-id";

export default function ChatWidget() {
  const [open, setOpen] = useState(false);
  const [minimized, setMinimized] = useState(false);
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const [sessionId, setSessionId] = useState<string | undefined>(
    () => localStorage.getItem(SESSION_KEY) || undefined,
  );
  const [error, setError] = useState<string | null>(null);
  const [unread, setUnread] = useState(0);
  const bottomRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLTextAreaElement>(null);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages, loading]);

  useEffect(() => {
    if (open && !minimized) {
      inputRef.current?.focus();
      setUnread(0);
    }
  }, [open, minimized]);

  const handleSend = async () => {
    const msg = input.trim();
    if (!msg || loading) return;

    setInput("");
    setError(null);
    setMessages((prev) => [...prev, { role: "user", content: msg }]);
    setLoading(true);

    try {
      const res = await sendChatMessage(msg, sessionId);
      setSessionId(res.session_id);
      localStorage.setItem(SESSION_KEY, res.session_id);
      setMessages(res.history);
      if (!open || minimized) setUnread((u) => u + 1);
    } catch (err) {
      const errMsg = err instanceof Error ? err.message : "Failed to send message";
      setError(errMsg);
      setMessages((prev) => [
        ...prev,
        { role: "assistant", content: `Error: ${errMsg}` },
      ]);
    } finally {
      setLoading(false);
      inputRef.current?.focus();
    }
  };

  const handleClear = async () => {
    if (sessionId) {
      try { await clearChatSession(sessionId); } catch { /* ignore */ }
    }
    setMessages([]);
    setSessionId(undefined);
    localStorage.removeItem(SESSION_KEY);
    setError(null);
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  };

  // Floating bubble when closed
  if (!open) {
    return (
      <button
        onClick={() => { setOpen(true); setMinimized(false); }}
        className="fixed bottom-5 right-5 z-[9999] h-14 w-14 rounded-full bg-blue-500 hover:bg-blue-600 text-white shadow-lg flex items-center justify-center transition-all hover:scale-105 active:scale-95"
        title="Chat with AI Assistant"
      >
        <MessageCircle className="h-6 w-6" />
        {unread > 0 && (
          <span className="absolute -top-1 -right-1 h-5 w-5 rounded-full bg-red-500 text-[10px] font-bold flex items-center justify-center">
            {unread}
          </span>
        )}
      </button>
    );
  }

  // Minimized bar
  if (minimized) {
    return (
      <div
        className="fixed bottom-5 right-5 z-[9999] w-72 rounded-xl bg-[var(--surface)] border border-[var(--border)] shadow-2xl cursor-pointer"
        onClick={() => setMinimized(false)}
      >
        <div className="flex items-center justify-between px-3 py-2.5">
          <div className="flex items-center gap-2">
            <div className="h-6 w-6 rounded-full bg-blue-500 flex items-center justify-center">
              <Bot className="h-3.5 w-3.5 text-white" />
            </div>
            <span className="text-xs font-medium text-[var(--text)]">DSE AI Chat</span>
            {unread > 0 && (
              <span className="h-4 w-4 rounded-full bg-red-500 text-[9px] text-white font-bold flex items-center justify-center">
                {unread}
              </span>
            )}
          </div>
          <button
            onClick={(e) => { e.stopPropagation(); setOpen(false); }}
            className="text-[var(--text-dim)] hover:text-[var(--text)]"
          >
            <X className="h-3.5 w-3.5" />
          </button>
        </div>
      </div>
    );
  }

  // Full chat panel
  return (
    <div className="fixed bottom-5 right-5 z-[9999] w-[380px] h-[560px] max-h-[80vh] rounded-xl bg-[var(--surface)] border border-[var(--border)] shadow-2xl flex flex-col overflow-hidden">
      {/* Header */}
      <div className="flex items-center justify-between px-3 py-2 border-b border-[var(--border)] bg-blue-500 rounded-t-xl shrink-0">
        <div className="flex items-center gap-2">
          <Bot className="h-4 w-4 text-white" />
          <span className="text-sm font-semibold text-white">DSE AI Assistant</span>
          <span className="text-[9px] text-blue-100 bg-blue-600 px-1.5 py-0.5 rounded">Claude</span>
        </div>
        <div className="flex items-center gap-1">
          <button
            onClick={handleClear}
            className="p-1 rounded text-blue-200 hover:text-white hover:bg-blue-600 transition-colors"
            title="Clear chat"
          >
            <Trash2 className="h-3.5 w-3.5" />
          </button>
          <button
            onClick={() => setMinimized(true)}
            className="p-1 rounded text-blue-200 hover:text-white hover:bg-blue-600 transition-colors"
            title="Minimize"
          >
            <Minus className="h-3.5 w-3.5" />
          </button>
          <button
            onClick={() => setOpen(false)}
            className="p-1 rounded text-blue-200 hover:text-white hover:bg-blue-600 transition-colors"
            title="Close"
          >
            <X className="h-3.5 w-3.5" />
          </button>
        </div>
      </div>

      {/* Messages */}
      <div className="flex-1 overflow-y-auto px-3 py-3 space-y-3">
        {messages.length === 0 && !loading && (
          <div className="flex flex-col items-center justify-center h-full text-center px-2">
            <Bot className="h-10 w-10 text-[var(--text-dim)] mb-2" />
            <p className="text-xs text-[var(--text-muted)] mb-1">
              Ask anything about DSE stocks
            </p>
            <p className="text-[10px] text-[var(--text-dim)] mb-3">
              I can query real market data, analyze stocks, and give trading advice
            </p>
            <div className="flex flex-wrap gap-1.5 justify-center">
              {[
                "How is the market today?",
                "Analyze ORIONINFU",
                "Best stocks under 50 BDT",
                "My portfolio P&L",
              ].map((q) => (
                <button
                  key={q}
                  onClick={() => {
                    setInput(q);
                    inputRef.current?.focus();
                  }}
                  className="text-[10px] px-2.5 py-1 rounded-full border border-[var(--border)] text-[var(--text-muted)] hover:text-[var(--text)] hover:bg-[var(--hover)] transition-colors"
                >
                  {q}
                </button>
              ))}
            </div>
          </div>
        )}

        {messages.map((msg, i) => (
          <div
            key={i}
            className={clsx(
              "flex gap-2",
              msg.role === "user" ? "flex-row-reverse" : "",
            )}
          >
            <div
              className={clsx(
                "shrink-0 h-6 w-6 rounded-full flex items-center justify-center text-white mt-0.5",
                msg.role === "user" ? "bg-blue-500" : "bg-emerald-600",
              )}
            >
              {msg.role === "user" ? (
                <User className="h-3 w-3" />
              ) : (
                <Bot className="h-3 w-3" />
              )}
            </div>
            <div
              className={clsx(
                "rounded-lg px-2.5 py-1.5 text-xs leading-relaxed max-w-[80%]",
                msg.role === "user"
                  ? "bg-blue-500 text-white"
                  : "bg-[var(--hover)] text-[var(--text)]",
              )}
            >
              <pre className="whitespace-pre-wrap font-sans break-words">
                {msg.content}
              </pre>
            </div>
          </div>
        ))}

        {loading && (
          <div className="flex gap-2">
            <div className="shrink-0 h-6 w-6 rounded-full flex items-center justify-center bg-emerald-600 text-white mt-0.5">
              <Bot className="h-3 w-3" />
            </div>
            <div className="rounded-lg px-2.5 py-1.5 bg-[var(--hover)] text-[var(--text-muted)] text-xs flex items-center gap-1.5">
              <Loader2 className="h-3 w-3 animate-spin" />
              Analyzing data... (30s-2min)
            </div>
          </div>
        )}

        {error && !loading && (
          <div className="text-[10px] text-red-500 text-center">{error}</div>
        )}

        <div ref={bottomRef} />
      </div>

      {/* Input */}
      <div className="shrink-0 border-t border-[var(--border)] px-3 py-2 bg-[var(--surface)]">
        <div className="flex gap-1.5">
          <textarea
            ref={inputRef}
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder="Ask about stocks, market, trading..."
            disabled={loading}
            rows={1}
            className={clsx(
              "flex-1 resize-none rounded-lg border border-[var(--border)] bg-[var(--bg)] text-[var(--text)] text-xs px-2.5 py-2",
              "placeholder:text-[var(--text-dim)] focus:outline-none focus:ring-1 focus:ring-blue-500",
              "disabled:opacity-50",
            )}
            style={{ minHeight: "2.25rem", maxHeight: "5rem" }}
            onInput={(e) => {
              const target = e.target as HTMLTextAreaElement;
              target.style.height = "auto";
              target.style.height = Math.min(target.scrollHeight, 80) + "px";
            }}
          />
          <button
            onClick={handleSend}
            disabled={!input.trim() || loading}
            className={clsx(
              "shrink-0 h-9 w-9 rounded-lg flex items-center justify-center transition-colors",
              input.trim() && !loading
                ? "bg-blue-500 text-white hover:bg-blue-600"
                : "bg-[var(--hover)] text-[var(--text-dim)] cursor-not-allowed",
            )}
          >
            {loading ? (
              <Loader2 className="h-3.5 w-3.5 animate-spin" />
            ) : (
              <Send className="h-3.5 w-3.5" />
            )}
          </button>
        </div>
      </div>
    </div>
  );
}
