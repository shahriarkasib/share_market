import { useState, useRef, useEffect, useCallback } from "react";
import { Send, Trash2, Loader2, MessageCircle, Bot, User, X, Minus, LogOut } from "lucide-react";
import { clsx } from "clsx";
import { sendChatMessage, clearChatSession } from "../../api/client.ts";
import type { ChatMessage } from "../../api/client.ts";

const SESSION_KEY = "dse-chat-session-id";
const USER_KEY = "dse-chat-user";
const GOOGLE_CLIENT_ID = import.meta.env.VITE_GOOGLE_CLIENT_ID || "";
const CHAT_URL = import.meta.env.VITE_CHAT_URL || "https://34.63.227.229.nip.io";

interface ChatUser {
  email: string;
  name: string;
  photo_url: string;
}

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
  const [user, setUser] = useState<ChatUser | null>(() => {
    const saved = localStorage.getItem(USER_KEY);
    return saved ? JSON.parse(saved) : null;
  });
  const bottomRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLTextAreaElement>(null);
  const googleBtnRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages, loading]);

  useEffect(() => {
    if (open && !minimized && user) {
      inputRef.current?.focus();
      setUnread(0);
    }
  }, [open, minimized, user]);

  // Load Google Sign-In script
  useEffect(() => {
    if (!GOOGLE_CLIENT_ID) return;
    const existing = document.getElementById("google-gsi-script");
    if (existing) return;

    const script = document.createElement("script");
    script.id = "google-gsi-script";
    script.src = "https://accounts.google.com/gsi/client";
    script.async = true;
    script.defer = true;
    document.head.appendChild(script);
  }, []);

  const handleGoogleResponse = useCallback(async (response: { credential: string }) => {
    try {
      // Decode JWT payload (base64)
      const payload = JSON.parse(atob(response.credential.split(".")[1]));
      const chatUser: ChatUser = {
        email: payload.email,
        name: payload.name,
        photo_url: payload.picture || "",
      };

      // Register with our backend
      await fetch(`${CHAT_URL}/auth/google`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(chatUser),
      });

      setUser(chatUser);
      localStorage.setItem(USER_KEY, JSON.stringify(chatUser));
    } catch (err) {
      console.error("Google auth failed:", err);
      setError("Sign-in failed. Try again.");
    }
  }, []);

  // Initialize Google button when needed
  useEffect(() => {
    if (!open || minimized || user || !GOOGLE_CLIENT_ID) return;
    const w = window as unknown as { google?: { accounts?: { id: { initialize: (opts: unknown) => void; renderButton: (el: HTMLElement, opts: unknown) => void } } } };
    if (!w.google?.accounts?.id) {
      // Wait for script to load
      const timer = setInterval(() => {
        const w2 = window as unknown as { google?: { accounts?: { id: { initialize: (opts: unknown) => void; renderButton: (el: HTMLElement, opts: unknown) => void } } } };
        if (w2.google?.accounts?.id && googleBtnRef.current) {
          clearInterval(timer);
          w2.google.accounts.id.initialize({
            client_id: GOOGLE_CLIENT_ID,
            callback: handleGoogleResponse,
          });
          w2.google.accounts.id.renderButton(googleBtnRef.current, {
            theme: "outline",
            size: "large",
            width: 280,
            text: "signin_with",
          });
        }
      }, 200);
      return () => clearInterval(timer);
    }
    if (googleBtnRef.current) {
      w.google.accounts.id.initialize({
        client_id: GOOGLE_CLIENT_ID,
        callback: handleGoogleResponse,
      });
      w.google.accounts.id.renderButton(googleBtnRef.current, {
        theme: "outline",
        size: "large",
        width: 280,
        text: "signin_with",
      });
    }
  }, [open, minimized, user, handleGoogleResponse]);

  const handleSend = async () => {
    const msg = input.trim();
    if (!msg || loading) return;

    setInput("");
    setError(null);
    setMessages((prev) => [...prev, { role: "user", content: msg }]);
    setLoading(true);

    try {
      const res = await sendChatMessage(msg, sessionId, user?.email);
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

  const handleSignOut = () => {
    setUser(null);
    localStorage.removeItem(USER_KEY);
    setMessages([]);
    setSessionId(undefined);
    localStorage.removeItem(SESSION_KEY);
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
        className="fixed bottom-5 right-5 z-[9999] w-[calc(100vw-2.5rem)] sm:w-72 rounded-xl bg-[var(--surface)] border border-[var(--border)] shadow-2xl cursor-pointer"
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
    <div className="fixed bottom-0 right-0 sm:bottom-5 sm:right-5 z-[9999] w-[calc(100vw-2.5rem)] sm:w-[380px] h-[calc(100vh-6rem)] sm:h-[560px] max-h-[80vh] rounded-none sm:rounded-xl bg-[var(--surface)] border border-[var(--border)] shadow-2xl flex flex-col overflow-hidden">
      {/* Header */}
      <div className="flex items-center justify-between px-3 py-2 border-b border-[var(--border)] bg-blue-500 rounded-t-none sm:rounded-t-xl shrink-0">
        <div className="flex items-center gap-2">
          {user?.photo_url ? (
            <img src={user.photo_url} alt="" className="h-5 w-5 rounded-full" />
          ) : (
            <Bot className="h-4 w-4 text-white" />
          )}
          <span className="text-sm font-semibold text-white">
            {user ? user.name.split(" ")[0] : "DSE AI"}
          </span>
          <span className="text-[9px] text-blue-100 bg-blue-600 px-1.5 py-0.5 rounded">Claude</span>
        </div>
        <div className="flex items-center gap-1">
          {user && (
            <button
              onClick={handleSignOut}
              className="p-1 rounded text-blue-200 hover:text-white hover:bg-blue-600 transition-colors"
              title="Sign out"
            >
              <LogOut className="h-3.5 w-3.5" />
            </button>
          )}
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

      {/* Login screen (no user) */}
      {!user && GOOGLE_CLIENT_ID ? (
        <div className="flex-1 flex flex-col items-center justify-center px-6 text-center">
          <Bot className="h-12 w-12 text-blue-500 mb-3" />
          <h3 className="text-sm font-semibold text-[var(--text)] mb-1">DSE AI Trading Assistant</h3>
          <p className="text-xs text-[var(--text-muted)] mb-4">
            Sign in to get personalized advice based on your portfolio and strategy
          </p>
          <div ref={googleBtnRef} className="mb-4" />
          <button
            onClick={() => {
              const anon: ChatUser = { email: "", name: "Guest", photo_url: "" };
              setUser(anon);
            }}
            className="text-[10px] text-[var(--text-dim)] hover:text-[var(--text)] underline"
          >
            Continue as guest
          </button>
        </div>
      ) : !user ? (
        /* No Google Client ID configured — simple name input */
        <div className="flex-1 flex flex-col items-center justify-center px-6 text-center">
          <Bot className="h-12 w-12 text-blue-500 mb-3" />
          <h3 className="text-sm font-semibold text-[var(--text)] mb-1">DSE AI Trading Assistant</h3>
          <p className="text-xs text-[var(--text-muted)] mb-4">
            Enter your name to get started
          </p>
          <form
            onSubmit={(e) => {
              e.preventDefault();
              const nameInput = (e.target as HTMLFormElement).elements.namedItem("name") as HTMLInputElement;
              const name = nameInput.value.trim();
              if (name) {
                const u: ChatUser = { email: `${name.toLowerCase().replace(/\s+/g, ".")}@guest`, name, photo_url: "" };
                fetch(`${CHAT_URL}/auth/google`, {
                  method: "POST",
                  headers: { "Content-Type": "application/json" },
                  body: JSON.stringify(u),
                }).catch(() => {});
                setUser(u);
                localStorage.setItem(USER_KEY, JSON.stringify(u));
              }
            }}
            className="flex gap-2 w-full max-w-[280px]"
          >
            <input
              name="name"
              placeholder="Your name"
              className="flex-1 rounded-lg border border-[var(--border)] bg-[var(--bg)] text-[var(--text)] text-xs px-3 py-2 focus:outline-none focus:ring-1 focus:ring-blue-500"
              autoFocus
            />
            <button
              type="submit"
              className="px-3 py-2 rounded-lg bg-blue-500 text-white text-xs font-medium hover:bg-blue-600"
            >
              Start
            </button>
          </form>
        </div>
      ) : (
        <>
          {/* Messages */}
          <div className="flex-1 overflow-y-auto px-3 py-3 space-y-3">
            {messages.length === 0 && !loading && (
              <div className="flex flex-col items-center justify-center h-full text-center px-2">
                <Bot className="h-10 w-10 text-[var(--text-dim)] mb-2" />
                <p className="text-xs text-[var(--text-muted)] mb-1">
                  Hi {user.name.split(" ")[0]}! Ask anything about DSE stocks
                </p>
                <p className="text-[10px] text-[var(--text-dim)] mb-3">
                  I can query real market data, analyze stocks, and give trading advice
                </p>
                <div className="flex flex-wrap gap-1.5 justify-center">
                  {[
                    "How is the market today?",
                    "Analyze my portfolio",
                    "Best stocks under 50 BDT",
                    "What should I buy?",
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
                    "shrink-0 h-6 w-6 rounded-full flex items-center justify-center text-white mt-0.5 overflow-hidden",
                    msg.role === "user" ? "bg-blue-500" : "bg-emerald-600",
                  )}
                >
                  {msg.role === "user" && user.photo_url ? (
                    <img src={user.photo_url} alt="" className="h-6 w-6" />
                  ) : msg.role === "user" ? (
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
        </>
      )}
    </div>
  );
}
