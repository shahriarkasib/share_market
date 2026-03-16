import { useState, useRef, useEffect } from "react";
import { Send, Trash2, Loader2, MessageCircle, Bot, User } from "lucide-react";
import { clsx } from "clsx";
import { sendChatMessage, clearChatSession } from "../api/client.ts";
import type { ChatMessage } from "../api/client.ts";

const SESSION_KEY = "dse-chat-session-id";

export default function Chat() {
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const [sessionId, setSessionId] = useState<string | undefined>(
    () => localStorage.getItem(SESSION_KEY) || undefined,
  );
  const [error, setError] = useState<string | null>(null);
  const bottomRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLTextAreaElement>(null);

  // Auto-scroll to bottom
  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages, loading]);

  // Focus input on mount
  useEffect(() => {
    inputRef.current?.focus();
  }, []);

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
      try {
        await clearChatSession(sessionId);
      } catch {
        // ignore
      }
    }
    setMessages([]);
    setSessionId(undefined);
    localStorage.removeItem(SESSION_KEY);
    setError(null);
    inputRef.current?.focus();
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  };

  return (
    <div className="flex flex-col h-[calc(100vh-3rem)]">
      {/* Header */}
      <div className="flex items-center justify-between px-4 py-2 border-b border-[var(--border)] bg-[var(--surface)] shrink-0">
        <div className="flex items-center gap-2">
          <MessageCircle className="h-4 w-4 text-blue-500" />
          <h1 className="text-sm font-semibold text-[var(--text)]">
            DSE Trading Chat
          </h1>
          <span className="text-[10px] text-[var(--text-dim)] bg-[var(--hover)] px-1.5 py-0.5 rounded">
            Powered by Claude
          </span>
        </div>
        <button
          onClick={handleClear}
          className="flex items-center gap-1 text-xs text-[var(--text-muted)] hover:text-red-500 transition-colors px-2 py-1 rounded hover:bg-[var(--hover)]"
          title="Clear chat"
        >
          <Trash2 className="h-3 w-3" />
          Clear
        </button>
      </div>

      {/* Messages */}
      <div className="flex-1 overflow-y-auto px-4 py-4 space-y-4">
        {messages.length === 0 && !loading && (
          <div className="flex flex-col items-center justify-center h-full text-center">
            <Bot className="h-12 w-12 text-[var(--text-dim)] mb-3" />
            <p className="text-sm text-[var(--text-muted)] mb-1">
              Ask anything about DSE stocks
            </p>
            <p className="text-xs text-[var(--text-dim)]">
              Market analysis, stock recommendations, portfolio advice, trading strategies
            </p>
            <div className="flex flex-wrap gap-2 mt-4 justify-center">
              {[
                "How is the market doing today?",
                "Best stocks to buy under 50 BDT?",
                "Analyze ORIONINFU",
                "What happens after Eid holiday?",
              ].map((q) => (
                <button
                  key={q}
                  onClick={() => {
                    setInput(q);
                    inputRef.current?.focus();
                  }}
                  className="text-xs px-3 py-1.5 rounded-full border border-[var(--border)] text-[var(--text-muted)] hover:text-[var(--text)] hover:bg-[var(--hover)] transition-colors"
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
              "flex gap-3 max-w-3xl",
              msg.role === "user" ? "ml-auto flex-row-reverse" : "",
            )}
          >
            <div
              className={clsx(
                "shrink-0 h-7 w-7 rounded-full flex items-center justify-center text-white",
                msg.role === "user" ? "bg-blue-500" : "bg-emerald-600",
              )}
            >
              {msg.role === "user" ? (
                <User className="h-3.5 w-3.5" />
              ) : (
                <Bot className="h-3.5 w-3.5" />
              )}
            </div>
            <div
              className={clsx(
                "rounded-lg px-3 py-2 text-sm leading-relaxed max-w-[85%]",
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
          <div className="flex gap-3 max-w-3xl">
            <div className="shrink-0 h-7 w-7 rounded-full flex items-center justify-center bg-emerald-600 text-white">
              <Bot className="h-3.5 w-3.5" />
            </div>
            <div className="rounded-lg px-3 py-2 bg-[var(--hover)] text-[var(--text-muted)] text-sm flex items-center gap-2">
              <Loader2 className="h-3.5 w-3.5 animate-spin" />
              Thinking... (this may take 10-30s)
            </div>
          </div>
        )}

        {error && !loading && (
          <div className="text-xs text-red-500 text-center">{error}</div>
        )}

        <div ref={bottomRef} />
      </div>

      {/* Input */}
      <div className="shrink-0 border-t border-[var(--border)] bg-[var(--surface)] px-4 py-3">
        <div className="flex gap-2 max-w-3xl mx-auto">
          <textarea
            ref={inputRef}
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder="Ask about DSE stocks, market analysis, trading advice..."
            disabled={loading}
            rows={1}
            className={clsx(
              "flex-1 resize-none rounded-lg border border-[var(--border)] bg-[var(--bg)] text-[var(--text)] text-sm px-3 py-2",
              "placeholder:text-[var(--text-dim)] focus:outline-none focus:ring-1 focus:ring-blue-500",
              "disabled:opacity-50",
            )}
            style={{ minHeight: "2.5rem", maxHeight: "8rem" }}
            onInput={(e) => {
              const target = e.target as HTMLTextAreaElement;
              target.style.height = "auto";
              target.style.height = Math.min(target.scrollHeight, 128) + "px";
            }}
          />
          <button
            onClick={handleSend}
            disabled={!input.trim() || loading}
            className={clsx(
              "shrink-0 h-10 w-10 rounded-lg flex items-center justify-center transition-colors",
              input.trim() && !loading
                ? "bg-blue-500 text-white hover:bg-blue-600"
                : "bg-[var(--hover)] text-[var(--text-dim)] cursor-not-allowed",
            )}
          >
            {loading ? (
              <Loader2 className="h-4 w-4 animate-spin" />
            ) : (
              <Send className="h-4 w-4" />
            )}
          </button>
        </div>
        <p className="text-[10px] text-[var(--text-dim)] text-center mt-1">
          Enter to send, Shift+Enter for new line
        </p>
      </div>
    </div>
  );
}
