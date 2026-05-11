import { Routes, Route } from "react-router-dom";
import Layout from "./components/layout/Layout.tsx";
import Dashboard from "./pages/Dashboard.tsx";
import StockDetail from "./pages/StockDetail.tsx";
import Heatmap from "./pages/Heatmap.tsx";
import DataMatrix from "./pages/DataMatrix.tsx";
import AdvancedChart from "./pages/AdvancedChart.tsx";
import SMCChart from "./pages/SMCChart.tsx";
import BuyRadar from "./pages/BuyRadar.tsx";
import Analysis from "./pages/Analysis.tsx";
import LiveSignals from "./pages/LiveSignals.tsx";
import News from "./pages/News.tsx";
import Seasonality from "./pages/Seasonality.tsx";
import Dividends from "./pages/Dividends.tsx";
import FloorDetection from "./pages/FloorDetection.tsx";
import Signals from "./pages/Signals.tsx";
import NasdaqSignals from "./pages/NasdaqSignals.tsx";
import LiveCompositeSignals from "./pages/LiveCompositeSignals.tsx";
import SmartMoney from "./pages/SmartMoney.tsx";
import SignalAccuracy from "./pages/SignalAccuracy.tsx";

export default function App() {
  return (
    <Routes>
      <Route element={<Layout />}>
        <Route path="/" element={<Dashboard />} />
        <Route path="/stock/:symbol" element={<StockDetail />} />
        <Route path="/heatmap" element={<Heatmap />} />
        <Route path="/matrix" element={<DataMatrix />} />
        <Route path="/chart" element={<AdvancedChart />} />
        <Route path="/smc-chart/:symbol?" element={<SMCChart />} />
        <Route path="/radar" element={<Analysis />} />
        <Route path="/live" element={<LiveSignals />} />
        <Route path="/radar-old" element={<BuyRadar />} />
        <Route path="/news" element={<News />} />
        <Route path="/seasonality" element={<Seasonality />} />
        <Route path="/dividends" element={<Dividends />} />
        <Route path="/floor" element={<FloorDetection />} />
        <Route path="/signals" element={<Signals />} />
        <Route path="/live-signals" element={<LiveCompositeSignals />} />
        <Route path="/smart-money" element={<SmartMoney />} />
        <Route path="/accuracy" element={<SignalAccuracy />} />
        <Route path="/nasdaq/signals" element={<NasdaqSignals />} />
        <Route path="/nasdaq/smc-chart/:symbol?" element={<SMCChart market="nasdaq" />} />
        <Route path="/nasdaq/live-signals" element={<LiveCompositeSignals market="nasdaq" />} />
        <Route path="/nasdaq/accuracy" element={<SignalAccuracy market="nasdaq" />} />
      </Route>
    </Routes>
  );
}
