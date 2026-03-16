import { Routes, Route } from "react-router-dom";
import Layout from "./components/layout/Layout.tsx";
import Dashboard from "./pages/Dashboard.tsx";
import StockDetail from "./pages/StockDetail.tsx";
import Heatmap from "./pages/Heatmap.tsx";
import DataMatrix from "./pages/DataMatrix.tsx";
import AdvancedChart from "./pages/AdvancedChart.tsx";
import DailyAnalysis from "./pages/DailyAnalysis.tsx";
import BuyRadar from "./pages/BuyRadar.tsx";
import News from "./pages/News.tsx";
import Seasonality from "./pages/Seasonality.tsx";
import Dividends from "./pages/Dividends.tsx";
import FloorDetection from "./pages/FloorDetection.tsx";

export default function App() {
  return (
    <Routes>
      <Route element={<Layout />}>
        <Route path="/" element={<Dashboard />} />
        <Route path="/stock/:symbol" element={<StockDetail />} />
        <Route path="/heatmap" element={<Heatmap />} />
        <Route path="/matrix" element={<DataMatrix />} />
        <Route path="/chart" element={<AdvancedChart />} />
        <Route path="/analysis" element={<DailyAnalysis />} />
        <Route path="/radar" element={<BuyRadar />} />
        <Route path="/news" element={<News />} />
        <Route path="/seasonality" element={<Seasonality />} />
        <Route path="/dividends" element={<Dividends />} />
        <Route path="/floor" element={<FloorDetection />} />
      </Route>
    </Routes>
  );
}
