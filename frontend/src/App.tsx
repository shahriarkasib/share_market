import { Routes, Route } from "react-router-dom";
import Layout from "./components/layout/Layout.tsx";
import Dashboard from "./pages/Dashboard.tsx";
import StockDetail from "./pages/StockDetail.tsx";
import Screener from "./pages/Screener.tsx";
import Watchlist from "./pages/Watchlist.tsx";
import Portfolio from "./pages/Portfolio.tsx";
import Heatmap from "./pages/Heatmap.tsx";
import SectorPerformancePage from "./pages/SectorPerformance.tsx";
import DataMatrix from "./pages/DataMatrix.tsx";
import AdvancedChart from "./pages/AdvancedChart.tsx";

export default function App() {
  return (
    <Routes>
      <Route element={<Layout />}>
        <Route path="/" element={<Dashboard />} />
        <Route path="/stock/:symbol" element={<StockDetail />} />
        <Route path="/screener" element={<Screener />} />
        <Route path="/heatmap" element={<Heatmap />} />
        <Route path="/sectors" element={<SectorPerformancePage />} />
        <Route path="/portfolio" element={<Portfolio />} />
        <Route path="/watchlist" element={<Watchlist />} />
        <Route path="/matrix" element={<DataMatrix />} />
        <Route path="/chart" element={<AdvancedChart />} />
      </Route>
    </Routes>
  );
}
