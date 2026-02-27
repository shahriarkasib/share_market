import { Outlet } from "react-router-dom";
import Header from "./Header.tsx";

export default function Layout() {
  return (
    <div className="min-h-screen bg-[var(--bg)] text-[var(--text)] flex flex-col">
      <Header />
      <main className="flex-1 px-4 py-4 lg:px-8 overflow-auto">
        <div className="max-w-[1440px] mx-auto">
          <Outlet />
        </div>
      </main>
    </div>
  );
}
