"""
Multi-day price prediction using ARMA, GARCH(1,1), and Bootstrap Monte Carlo.

Statistical methods (all proven, peer-reviewed):

1. ARMA(p,q) — Conditional mean forecast with AIC-based order selection.
   AR via Yule-Walker, ARMA via Conditional Sum-of-Squares MLE.
   Reference: Box, Jenkins, Reinsel (2015) "Time Series Analysis"

2. GARCH(1,1) — Volatility modeling for prediction intervals via MLE.
   σ²_t = ω + α·ε²_{t-1} + β·σ²_{t-1}
   Reference: Bollerslev (1986) "Generalized Autoregressive Conditional
   Heteroskedasticity", Journal of Econometrics 31(3), 307-327.

3. Bootstrap Monte Carlo — Block-bootstrap resampling of historical returns,
   N simulated forward paths. Median = point prediction, percentiles = intervals.
   Reference: Efron & Tibshirani (1993) "An Introduction to the Bootstrap"
   Politis & Romano (1994) "The Stationary Bootstrap", JASA 89(428).

Ensemble: Inverse-variance weighting from each method's prediction variance.
Reference: Timmermann (2006) "Forecast Combinations",
Handbook of Economic Forecasting, Vol 1, Ch 4.
"""

import numpy as np
import pandas as pd
import logging
from scipy.optimize import minimize
from config import PREDICTION_DAYS, SR_PIVOT_WINDOW, SR_CLUSTER_PCT

logger = logging.getLogger(__name__)

# --- Predictor configuration ---
MAX_AR_ORDER = 3       # Maximum AR order for AIC grid search
MAX_MA_ORDER = 2       # Maximum MA order for AIC grid search
MIN_DATA_POINTS = 60   # Minimum OHLCV rows required
MC_SIMULATIONS = 3000  # Bootstrap Monte Carlo paths
MC_BLOCK_SIZE = 5      # Block bootstrap block length (preserves autocorrelation)
GARCH_MAX_ITER = 1000  # MLE iterations for GARCH fitting


class PricePredictor:
    """
    Predicts future prices using three proven statistical methods:

    1. ARMA(p,q) with AIC selection — captures mean-reversion and momentum
       in returns. Orders selected by minimizing AIC over a grid search.

    2. GARCH(1,1) — models time-varying volatility via MLE.
       Generates proper prediction intervals that widen with horizon.

    3. Bootstrap Monte Carlo — block-resamples historical returns,
       simulates N forward paths. Distribution-free (captures fat tails).

    Ensemble: Methods weighted by inverse prediction variance.
    """

    def __init__(self, df: pd.DataFrame, indicators: dict | None = None):
        self.df = df
        self.closes = df["close"].values.astype(float)
        self.highs = df["high"].values.astype(float)
        self.lows = df["low"].values.astype(float)
        self.volumes = df["volume"].values.astype(float)
        self.current = self.closes[-1]
        self.indicators = indicators or {}

        # Precompute log-returns (used by all three methods)
        safe_closes = np.where(self.closes <= 0, 1e-10, self.closes)
        self.log_returns = np.diff(np.log(safe_closes))

    def predict(self) -> dict:
        """Run all statistical methods and return ensemble prediction."""
        if len(self.closes) < MIN_DATA_POINTS:
            return self._empty_prediction()

        try:
            arma_result = self._arma_predict()
            garch_result = self._garch_predict()
            mc_result = self._bootstrap_monte_carlo()

            # Compute inverse-variance ensemble weights
            weights = self._inverse_variance_weights(arma_result, mc_result)

            # Support/resistance (standard pivot-point detection)
            sr = self._support_resistance()

            return self._ensemble(arma_result, mc_result, garch_result, weights, sr)

        except Exception as e:
            logger.error(f"Prediction error: {e}", exc_info=True)
            return self._empty_prediction()

    # ------------------------------------------------------------------ #
    #  Method 1: ARMA(p,q) with AIC Model Selection                       #
    # ------------------------------------------------------------------ #

    def _arma_predict(self) -> dict:
        """
        Fit ARMA(p,q) on log-returns, select best order by AIC.
        Grid search over p ∈ [0, MAX_AR_ORDER], q ∈ [0, MAX_MA_ORDER].
        Pure AR models use fast Yule-Walker; MA > 0 uses CSS-MLE.

        Returns point predictions and residual variance for each horizon.
        """
        returns = self.log_returns
        n = len(returns)

        if n < 30:
            return self._flat_arma_result()

        best_aic = np.inf
        best_order = (1, 0)
        best_params = None

        for p in range(MAX_AR_ORDER + 1):
            for q in range(MAX_MA_ORDER + 1):
                if p == 0 and q == 0:
                    continue
                try:
                    if q == 0:
                        # Pure AR: fast Yule-Walker (analytical, no optimization)
                        params, aic = self._fit_ar_yw(returns, p)
                    else:
                        # ARMA: Conditional Sum-of-Squares MLE
                        params, aic = self._fit_arma_css(returns, p, q)

                    if aic < best_aic:
                        best_aic = aic
                        best_order = (p, q)
                        best_params = params
                except Exception:
                    continue

        if best_params is None:
            return self._flat_arma_result()

        p, q = best_order
        mu = best_params["mu"]
        ar_coeffs = best_params.get("ar", np.array([]))
        ma_coeffs = best_params.get("ma", np.array([]))
        sigma2 = best_params["sigma2"]

        logger.debug(f"Selected ARMA({p},{q}), AIC={best_aic:.1f}")

        # Compute residuals for MA forecasting
        residuals = self._arma_residuals(returns, mu, ar_coeffs, ma_coeffs)

        # Multi-step ahead forecasting
        max_day = max(PREDICTION_DAYS)
        recent_returns = list(returns[-max(p, 1):])
        recent_residuals = list(residuals[-max(q, 1):]) if q > 0 else []

        predicted_log_returns = []
        for step in range(max_day):
            pred = mu
            # AR component
            for i in range(p):
                idx = len(recent_returns) - 1 - i
                if idx >= 0:
                    pred += ar_coeffs[i] * (recent_returns[idx] - mu)
            # MA component (future residuals = 0, only past residuals contribute)
            for j in range(q):
                residual_idx = len(recent_residuals) - 1 - j
                if residual_idx >= 0 and (step - 1 - j) < 0:
                    pred += ma_coeffs[j] * recent_residuals[residual_idx]

            predicted_log_returns.append(pred)
            recent_returns.append(pred)
            if q > 0:
                recent_residuals.append(0.0)

        # Convert cumulative log-returns to prices
        predictions = {}
        cum = 0.0
        for d in range(1, max_day + 1):
            cum += predicted_log_returns[d - 1]
            if d in PREDICTION_DAYS:
                predictions[f"day_{d}"] = self.current * np.exp(cum)

        # Prediction variance via psi-weights (MA(∞) representation)
        # var(e_h) = σ² · Σ(ψ_j², j=0..h-1)
        psi = self._psi_weights(ar_coeffs, ma_coeffs, max_day)
        variances = {}
        for d in PREDICTION_DAYS:
            # Cumulative variance for d-step-ahead forecast
            var_h = sigma2 * sum(psi[j] ** 2 for j in range(d))
            variances[f"day_{d}"] = var_h

        return {"predictions": predictions, "variances": variances, "sigma2": sigma2}

    def _fit_ar_yw(self, returns: np.ndarray, p: int) -> tuple[dict, float]:
        """
        Fit pure AR(p) via Yule-Walker equations (analytical, instant).

        Solves: R · φ = r where R is the autocorrelation Toeplitz matrix
        and r is the autocorrelation vector [acf(1)..acf(p)].

        Returns (params_dict, AIC).
        """
        n = len(returns)
        mu = np.mean(returns)
        centered = returns - mu

        gamma = np.correlate(centered, centered, mode="full")
        gamma = gamma[n - 1:]  # Keep positive lags only
        gamma0 = gamma[0]

        if gamma0 <= 0 or len(gamma) < p + 1:
            raise ValueError("Insufficient autocorrelation structure")

        acf = gamma / gamma0

        # Toeplitz system
        R = np.zeros((p, p))
        for i in range(p):
            for j in range(p):
                R[i, j] = acf[abs(i - j)]
        r = acf[1: p + 1]

        try:
            phi = np.linalg.solve(R, r)
        except np.linalg.LinAlgError:
            raise ValueError("Singular autocorrelation matrix")

        # Check stationarity: all roots of 1 - φ_1·z - ... - φ_p·z^p outside unit circle
        ar_poly = np.concatenate([[1.0], -phi])
        roots = np.roots(ar_poly)
        if np.any(np.abs(roots) <= 1.0):
            raise ValueError("AR model not stationary")

        # Residual variance
        residuals = self._arma_residuals(returns, mu, phi, np.array([]))
        sigma2 = np.mean(residuals ** 2)

        # AIC = 2k + n·log(σ²) (conditional AIC form)
        k = p + 2  # p AR coeffs + mu + sigma2
        aic = 2 * k + n * np.log(sigma2 + 1e-15)

        params = {"mu": mu, "ar": phi, "sigma2": sigma2}
        return params, aic

    def _fit_arma_css(self, returns: np.ndarray, p: int, q: int) -> tuple[dict, float]:
        """
        Fit ARMA(p,q) via Conditional Sum-of-Squares MLE.
        Uses Nelder-Mead optimization.

        Returns (params_dict, AIC).
        """
        n = len(returns)
        mu_init = np.mean(returns)

        # Initialize AR from Yule-Walker if possible
        ar_init = np.zeros(p)
        if p > 0:
            try:
                yw_params, _ = self._fit_ar_yw(returns, p)
                ar_init = yw_params["ar"]
            except Exception:
                pass

        ma_init = np.zeros(q)
        sigma2_init = np.var(returns)

        # Pack: [mu, ar_1..ar_p, ma_1..ma_q, log(sigma2)]
        x0 = np.concatenate([[mu_init], ar_init, ma_init, [np.log(sigma2_init + 1e-15)]])

        def neg_log_lik(params):
            mu = params[0]
            ar = params[1: 1 + p]
            ma = params[1 + p: 1 + p + q]
            log_s2 = params[-1]
            s2 = np.exp(log_s2)
            if s2 < 1e-15:
                return 1e10

            # Stationarity check
            if p > 0:
                ar_poly = np.concatenate([[1.0], -ar])
                roots = np.roots(ar_poly)
                if np.any(np.abs(roots) <= 1.01):
                    return 1e10

            # Invertibility check for MA
            if q > 0:
                ma_poly = np.concatenate([[1.0], ma])
                roots = np.roots(ma_poly)
                if np.any(np.abs(roots) <= 1.01):
                    return 1e10

            residuals = self._arma_residuals(returns, mu, ar, ma)
            css = np.sum(residuals ** 2)
            nll = 0.5 * n * np.log(2 * np.pi * s2) + css / (2 * s2)
            return nll

        result = minimize(
            neg_log_lik, x0, method="Nelder-Mead",
            options={"maxiter": 500, "xatol": 1e-6, "fatol": 1e-6},
        )

        if result.fun > 1e9:
            raise ValueError("ARMA CSS fitting failed")

        params_vec = result.x
        mu = params_vec[0]
        ar = params_vec[1: 1 + p]
        ma = params_vec[1 + p: 1 + p + q]
        sigma2 = np.exp(params_vec[-1])

        k = 1 + p + q + 1
        aic = 2 * k + 2 * result.fun

        params = {"mu": mu, "ar": ar, "ma": ma, "sigma2": sigma2}
        return params, aic

    def _arma_residuals(self, returns: np.ndarray, mu: float,
                        ar: np.ndarray, ma: np.ndarray) -> np.ndarray:
        """Compute ARMA residuals via recursive filtering."""
        n = len(returns)
        p = len(ar)
        q = len(ma)
        residuals = np.zeros(n)

        for t in range(n):
            pred = mu
            for i in range(p):
                if t - 1 - i >= 0:
                    pred += ar[i] * (returns[t - 1 - i] - mu)
            for j in range(q):
                if t - 1 - j >= 0:
                    pred += ma[j] * residuals[t - 1 - j]
            residuals[t] = returns[t] - pred

        return residuals

    def _psi_weights(self, ar: np.ndarray, ma: np.ndarray,
                     max_lag: int) -> np.ndarray:
        """
        Compute MA(∞) ψ-weights for ARMA prediction interval width.

        ψ_0 = 1
        ψ_j = Σ_{i=1}^{min(j,p)} ar_i · ψ_{j-i} + θ_j  (θ_j=0 if j>q)

        Reference: Box & Jenkins (2015), Ch. 5 — "Forecasting".
        """
        p = len(ar)
        q = len(ma)
        psi = np.zeros(max_lag + 1)
        psi[0] = 1.0

        for j in range(1, max_lag + 1):
            val = 0.0
            for i in range(min(j, p)):
                val += ar[i] * psi[j - 1 - i]
            if j <= q:
                val += ma[j - 1]
            psi[j] = val

        return psi

    # ------------------------------------------------------------------ #
    #  Method 2: GARCH(1,1) Volatility Model                              #
    # ------------------------------------------------------------------ #

    def _garch_predict(self) -> dict:
        """
        Fit GARCH(1,1) via Maximum Likelihood Estimation.

        Model: σ²_t = ω + α · ε²_{t-1} + β · σ²_{t-1}
        Constraints: ω > 0, α ≥ 0, β ≥ 0, α + β < 1 (covariance stationarity)

        Conditional variance forecast at horizon h:
            σ²_{t+h|t} = V_L + (α+β)^h · (σ²_t - V_L)
        where V_L = ω / (1 - α - β) is the long-run (unconditional) variance.

        Reference: Bollerslev (1986), J. of Econometrics 31(3), 307-327.
        """
        returns = self.log_returns
        n = len(returns)

        if n < 50:
            # Not enough data: fallback to simple variance scaling
            var = np.var(returns) if n > 1 else 0.001
            variances = {f"day_{d}": var * d for d in PREDICTION_DAYS}
            return {"variances": variances, "volatility_level": "MEDIUM"}

        mu = np.mean(returns)
        eps = returns - mu  # Mean-centered innovations

        # --- MLE fitting ---
        unconditional_var = np.var(eps)
        omega_init = unconditional_var * 0.05
        alpha_init = 0.10
        beta_init = 0.85

        def garch_neg_loglik(params):
            omega, alpha, beta = params
            if omega <= 1e-12 or alpha < 0 or beta < 0 or alpha + beta >= 0.9999:
                return 1e10

            sigma2 = np.empty(n)
            sigma2[0] = unconditional_var

            for t in range(1, n):
                sigma2[t] = omega + alpha * eps[t - 1] ** 2 + beta * sigma2[t - 1]
                if sigma2[t] < 1e-15:
                    sigma2[t] = 1e-15

            # Gaussian log-likelihood
            ll = -0.5 * np.sum(np.log(2 * np.pi * sigma2) + eps ** 2 / sigma2)
            return -ll  # Negative for minimization

        try:
            result = minimize(
                garch_neg_loglik,
                x0=[omega_init, alpha_init, beta_init],
                method="Nelder-Mead",
                options={"maxiter": GARCH_MAX_ITER, "xatol": 1e-8, "fatol": 1e-8},
            )
            omega, alpha, beta = result.x

            if omega <= 0 or alpha < 0 or beta < 0 or alpha + beta >= 1:
                raise ValueError("GARCH parameters out of stationarity bounds")

        except Exception:
            # Fallback: simple variance scaling (σ² grows linearly with horizon)
            variances = {f"day_{d}": unconditional_var * d for d in PREDICTION_DAYS}
            return {"variances": variances, "volatility_level": "MEDIUM"}

        # Compute final conditional variance σ²_T
        sigma2_T = unconditional_var
        for t in range(n):
            sigma2_T = omega + alpha * eps[t] ** 2 + beta * sigma2_T

        # Long-run variance V_L = ω / (1 - α - β)
        persistence = alpha + beta
        long_run_var = omega / (1 - persistence)

        # Multi-step variance forecast: σ²_{T+h} = V_L + (α+β)^h · (σ²_T - V_L)
        # Cumulative variance for h-day-ahead return:
        # Var(r_{T+1} + ... + r_{T+h}) ≈ Σ_{i=1}^{h} σ²_{T+i|T}
        variances = {}
        for d in PREDICTION_DAYS:
            cum_var = 0.0
            for h in range(1, d + 1):
                var_h = long_run_var + (persistence ** h) * (sigma2_T - long_run_var)
                cum_var += max(var_h, 1e-15)
            variances[f"day_{d}"] = cum_var

        # Classify annualized volatility
        daily_vol = np.sqrt(sigma2_T)
        annual_vol = daily_vol * np.sqrt(252)
        if annual_vol < 0.20:
            vol_level = "LOW"
        elif annual_vol < 0.40:
            vol_level = "MEDIUM"
        else:
            vol_level = "HIGH"

        return {
            "variances": variances,
            "sigma2_T": sigma2_T,
            "long_run_var": long_run_var,
            "volatility_level": vol_level,
        }

    # ------------------------------------------------------------------ #
    #  Method 3: Block Bootstrap Monte Carlo                               #
    # ------------------------------------------------------------------ #

    def _bootstrap_monte_carlo(self) -> dict:
        """
        Block-bootstrap Monte Carlo simulation for distribution-free
        point predictions and prediction intervals.

        Algorithm:
        1. Divide historical log-returns into overlapping blocks of size B
        2. For each simulation: randomly sample blocks (with replacement)
           to build a forward path of length max_day
        3. Compute cumulative returns → simulated price paths
        4. Median of paths = point prediction (robust to outliers)
        5. Percentiles = prediction intervals (e.g., 10th/90th = 80% PI)

        Block bootstrap preserves short-term autocorrelation in returns,
        which simple i.i.d. bootstrap destroys.

        Reference: Politis & Romano (1994), JASA 89(428), 1303-1313.
        """
        returns = self.log_returns
        n = len(returns)

        if n < 30:
            return self._flat_mc_result()

        max_day = max(PREDICTION_DAYS)
        block_size = min(MC_BLOCK_SIZE, n // 6)
        if block_size < 1:
            block_size = 1

        # Vectorized block bootstrap
        n_sims = MC_SIMULATIONS
        sim_paths = np.zeros((n_sims, max_day))

        # Pre-generate random block start indices
        blocks_needed = (max_day // block_size) + 2
        starts = np.random.randint(0, max(1, n - block_size), size=(n_sims, blocks_needed))

        for sim in range(n_sims):
            path = np.empty(0)
            for b in range(blocks_needed):
                s = starts[sim, b]
                block = returns[s: s + block_size]
                path = np.concatenate([path, block])
                if len(path) >= max_day:
                    break
            sim_paths[sim, :] = path[:max_day]

        # Cumulative log-returns → price paths
        cum_returns = np.cumsum(sim_paths, axis=1)
        sim_prices = self.current * np.exp(cum_returns)

        # Extract statistics per prediction day
        predictions = {}
        intervals = {}
        variances = {}

        for d in PREDICTION_DAYS:
            col = d - 1  # 0-indexed
            day_prices = sim_prices[:, col]

            # Median (robust point prediction)
            predictions[f"day_{d}"] = float(np.median(day_prices))

            # 80% prediction interval (10th & 90th percentiles)
            p10 = float(np.percentile(day_prices, 10))
            p90 = float(np.percentile(day_prices, 90))
            intervals[f"day_{d}"] = {"min": p10, "max": p90}

            # Empirical variance of log-returns at this horizon
            log_rets = np.log(day_prices / self.current)
            variances[f"day_{d}"] = float(np.var(log_rets))

        return {
            "predictions": predictions,
            "intervals": intervals,
            "variances": variances,
        }

    # ------------------------------------------------------------------ #
    #  Inverse-Variance Ensemble Weighting                                 #
    # ------------------------------------------------------------------ #

    def _inverse_variance_weights(self, arma_result: dict, mc_result: dict) -> dict:
        """
        Weight methods by inverse prediction variance.

        Weight_i = (1 / σ²_i) / Σ_j (1 / σ²_j)

        Lower-variance (more confident) predictions get higher weight.
        GARCH only produces variance, not point predictions, so it's
        excluded from point-prediction weighting.

        Reference: Timmermann (2006), "Forecast Combinations",
        Handbook of Economic Forecasting, Vol 1, Ch 4.
        """
        eps = 1e-12
        weights = {}

        for d in PREDICTION_DAYS:
            key = f"day_{d}"
            arma_var = arma_result.get("variances", {}).get(key, 0.001)
            mc_var = mc_result.get("variances", {}).get(key, 0.001)

            inv_arma = 1.0 / (arma_var + eps)
            inv_mc = 1.0 / (mc_var + eps)
            total = inv_arma + inv_mc

            weights[d] = {
                "arma": inv_arma / total,
                "mc": inv_mc / total,
            }

        return weights

    # ------------------------------------------------------------------ #
    #  Ensemble Combiner                                                   #
    # ------------------------------------------------------------------ #

    def _ensemble(self, arma_result, mc_result, garch_result, weights, sr) -> dict:
        """
        Combine predictions using inverse-variance weights.

        Point predictions: weighted average of ARMA + Monte Carlo.
        Prediction intervals: union of MC quantile intervals and
        GARCH-based parametric intervals (take the wider of the two).
        """
        predicted_prices = {}
        daily_ranges = {}

        for day in PREDICTION_DAYS:
            key = f"day_{day}"
            w = weights.get(day, {"arma": 0.5, "mc": 0.5})

            # Point predictions from ARMA and Bootstrap MC
            arma_price = arma_result.get("predictions", {}).get(key, self.current)
            mc_price = mc_result.get("predictions", {}).get(key, self.current)

            combined = arma_price * w["arma"] + mc_price * w["mc"]

            # Round to DSE minimum tick (0.10 BDT)
            predicted_prices[key] = round(combined * 10) / 10

            # --- Prediction intervals ---
            # Source 1: Monte Carlo quantiles (distribution-free)
            mc_intervals = mc_result.get("intervals", {}).get(key, {})
            mc_lo = mc_intervals.get("min", combined * 0.97)
            mc_hi = mc_intervals.get("max", combined * 1.03)

            # Source 2: GARCH parametric intervals
            # Using ±1.28σ for 80% prediction interval (matches MC's 10th/90th)
            garch_var = garch_result.get("variances", {}).get(key, 0)
            garch_std = np.sqrt(garch_var) if garch_var > 0 else 0

            if garch_std > 0:
                garch_lo = self.current * np.exp(-1.28 * garch_std)
                garch_hi = self.current * np.exp(1.28 * garch_std)
            else:
                garch_lo = mc_lo
                garch_hi = mc_hi

            # Take the wider of the two (conservative)
            lo = min(mc_lo, garch_lo)
            hi = max(mc_hi, garch_hi)

            # Round to DSE tick
            daily_ranges[key] = {
                "min": round(lo * 10) / 10,
                "max": round(hi * 10) / 10,
            }

        # Backwards-compatible 3-day range
        price_range_3d = daily_ranges.get("day_3", {
            "min": round(self.current * 0.97 * 10) / 10,
            "max": round(self.current * 1.03 * 10) / 10,
        })

        trend_strength = self._classify_trend(predicted_prices)
        vol_level = garch_result.get("volatility_level", "MEDIUM")

        # S/R levels (round to DSE tick)
        sup = sr.get("support")
        res = sr.get("resistance")

        return {
            "predicted_prices": predicted_prices,
            "daily_ranges": daily_ranges,
            "price_range_next_3d": price_range_3d,
            "support_level": round(sup * 10) / 10 if sup else 0,
            "resistance_level": round(res * 10) / 10 if res else 0,
            "trend_strength": trend_strength,
            "volatility_level": vol_level,
        }

    # ------------------------------------------------------------------ #
    #  Support / Resistance (standard pivot-point detection)               #
    # ------------------------------------------------------------------ #

    def _support_resistance(self) -> dict:
        """Detect S/R from local pivot highs/lows, cluster nearby levels."""
        window = SR_PIVOT_WINDOW
        n = len(self.closes)

        if n < window * 2 + 1:
            return {"support": None, "resistance": None}

        support_levels = []
        for i in range(window, n - window):
            local_lows = self.lows[i - window: i + window + 1]
            if self.lows[i] == np.min(local_lows):
                recency = i / n
                support_levels.append((self.lows[i], 0.5 + recency))

        resistance_levels = []
        for i in range(window, n - window):
            local_highs = self.highs[i - window: i + window + 1]
            if self.highs[i] == np.max(local_highs):
                recency = i / n
                resistance_levels.append((self.highs[i], 0.5 + recency))

        support = self._find_nearest_level(support_levels, self.current, below=True)
        resistance = self._find_nearest_level(resistance_levels, self.current, below=False)

        return {"support": support, "resistance": resistance}

    # ------------------------------------------------------------------ #
    #  Helpers                                                             #
    # ------------------------------------------------------------------ #

    def _classify_trend(self, predicted_prices: dict) -> str:
        """Classify trend based on predicted price trajectory."""
        prices = [predicted_prices.get(f"day_{d}", self.current) for d in PREDICTION_DAYS]
        if not prices:
            return "SIDEWAYS"

        total_change_pct = (
            (prices[-1] - self.current) / self.current * 100
            if self.current > 0 else 0
        )

        if abs(total_change_pct) < 0.5:
            return "SIDEWAYS"
        elif total_change_pct > 2.0:
            return "STRONG_UP"
        elif total_change_pct > 0.5:
            return "UP"
        elif total_change_pct < -2.0:
            return "STRONG_DOWN"
        else:
            return "DOWN"

    def _find_nearest_level(self, levels: list[tuple], current: float,
                            below: bool) -> float | None:
        """Find nearest S/R level above or below current price."""
        if not levels:
            return None

        clustered = self._cluster_levels(levels)

        if below:
            candidates = [(p, w) for p, w in clustered if p < current]
            if not candidates:
                return None
            candidates.sort(key=lambda x: -x[0])
            return candidates[0][0]
        else:
            candidates = [(p, w) for p, w in clustered if p > current]
            if not candidates:
                return None
            candidates.sort(key=lambda x: x[0])
            return candidates[0][0]

    @staticmethod
    def _cluster_levels(levels: list[tuple]) -> list[tuple]:
        """Cluster nearby price levels, combining their weights."""
        if not levels:
            return []

        sorted_levels = sorted(levels, key=lambda x: x[0])
        clusters = []
        current_prices = [sorted_levels[0][0]]
        current_weights = [sorted_levels[0][1]]

        for i in range(1, len(sorted_levels)):
            price, weight = sorted_levels[i]
            cluster_avg = np.mean(current_prices)
            if cluster_avg > 0 and abs(price - cluster_avg) / cluster_avg < SR_CLUSTER_PCT:
                current_prices.append(price)
                current_weights.append(weight)
            else:
                avg_price = np.average(current_prices, weights=current_weights)
                total_weight = sum(current_weights)
                clusters.append((avg_price, total_weight))
                current_prices = [price]
                current_weights = [weight]

        if current_prices:
            avg_price = np.average(current_prices, weights=current_weights)
            total_weight = sum(current_weights)
            clusters.append((avg_price, total_weight))

        return clusters

    def _flat_arma_result(self) -> dict:
        """Fallback ARMA result when data is insufficient."""
        var = np.var(self.log_returns) if len(self.log_returns) > 1 else 0.001
        return {
            "predictions": {f"day_{d}": self.current for d in PREDICTION_DAYS},
            "variances": {f"day_{d}": var * d for d in PREDICTION_DAYS},
            "sigma2": var,
        }

    def _flat_mc_result(self) -> dict:
        """Fallback MC result when data is insufficient."""
        return {
            "predictions": {f"day_{d}": self.current for d in PREDICTION_DAYS},
            "intervals": {
                f"day_{d}": {
                    "min": round(self.current * 0.97 * 10) / 10,
                    "max": round(self.current * 1.03 * 10) / 10,
                }
                for d in PREDICTION_DAYS
            },
            "variances": {f"day_{d}": 0.001 for d in PREDICTION_DAYS},
        }

    def _empty_prediction(self) -> dict:
        """Default prediction when insufficient data."""
        return {
            "predicted_prices": {f"day_{d}": 0 for d in PREDICTION_DAYS},
            "daily_ranges": {},
            "price_range_next_3d": {"min": 0, "max": 0},
            "support_level": 0,
            "resistance_level": 0,
            "trend_strength": "SIDEWAYS",
            "volatility_level": "MEDIUM",
        }
