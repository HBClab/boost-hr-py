use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct OlsParams {
    pub beta0: f64,
    pub beta1: f64,
    pub sigma2: f64, // residual variance estimate
    pub s00: f64,
    pub s01: f64,
    pub s11: f64, // elements of (X' W X)^{-1} for SEs
    pub n: usize,
}

#[derive(Debug, Clone, Copy)]
pub enum Weighting {
    None,
    N,
    Binomial,
}

impl OlsParams {
    pub fn predict(&self, x: f64) -> f64 {
        self.beta0 + self.beta1 * x
    }

    pub fn se_mean(&self, x: f64) -> f64 {
        // Var(ŷ) = σ^2 * [1, x] (X'WX)^{-1} [1, x]^T
        let v = self.s00 + 2.0 * x * self.s01 + x * x * self.s11;
        (self.sigma2 * v).sqrt()
    }

    pub fn se_pred(&self, x: f64) -> f64 {
        // Prediction SE: includes residual variance
        let se_mean = self.se_mean(x);
        (se_mean * se_mean + self.sigma2).sqrt()
    }
}

/// Fit OLS / WLS with intercept and one predictor using closed forms.
/// x: supervised adherence proportion in [0,1]
/// y: unsupervised adherence proportion in [0,1]
/// w: optional weights (None => all 1.0)
pub fn fit_wls(x: &[f64], y: &[f64], w: Option<&[f64]>) -> anyhow::Result<OlsParams> {
    if x.len() != y.len() {
        anyhow::bail!("x and y lengths differ");
    }
    let n = x.len();
    if n < 2 { anyhow::bail!("need at least 2 observations"); }
    

    // Accumulate weighted sums
    let mut s = 0.0; // Σ w
    let mut sx = 0.0; // Σ w x
    let mut sy = 0.0; // Σ w y
    let mut sxx = 0.0; // Σ w x^2
    let mut sxy = 0.0; // Σ w x y

    let ws: Vec<f64> = match w {
        Some(ws) => ws.to_vec(),
        None => vec![1.0; n],
    };

    for i in 0..n {
        let wi = ws[i];
        let xi = x[i];
        let yi = y[i];
        s += wi;
        sx += wi * xi;
        sy += wi * yi;
        sxx += wi * xi * xi;
        sxy += wi * xi * yi;
    }

    let det = s * sxx - sx * sx;
    if det.abs() < 1e-12 { anyhow::bail!("singular design (no variation in x?)"); }

    let beta1 = (s * sxy - sx * sy) / det;
    let beta0 = (sy - beta1 * sx) / s;

    // Residual variance estimate (use n-2 DF)
    let mut rss = 0.0;
    for i in 0..n {
        let yi = y[i];
        let xi = x[i];
        let e = yi - (beta0 + beta1 * xi);
        rss += ws[i] * e * e;
    }
    let df = (n as i32 - 2).max(1) as f64;
    let sigma2 = rss / df;

    // (X' W X)^{-1} for 2×2: inv = 1/det * [[sxx, -sx], [-sx, s]]
    let s00 =  sxx / det;    // var of intercept part
    let s01 = -sx  / det;    // cov(intercept, slope)
    let s11 =  s   / det;    // var of slope part

    Ok(OlsParams { beta0, beta1, sigma2, s00, s01, s11, n })
}

/// Build weights from unsup_den and y according to strategy.
pub fn make_weights(y: &[f64], unsup_den: Option<&[usize]>, strategy: Weighting) -> anyhow::Result<Option<Vec<f64>>> {
    match strategy {
        Weighting::None => Ok(None),
        Weighting::N => {
            let den = unsup_den.ok_or_else(|| anyhow::anyhow!("unsup_den required for N weighting"))?;
            Ok(Some(den.iter().map(|&m| (m as f64).max(1.0)).collect()))
        }
        Weighting::Binomial => {
            let den = unsup_den.ok_or_else(|| anyhow::anyhow!("unsup_den required for binomial weighting"))?;
            if den.len() != y.len() { anyhow::bail!("unsup_den length mismatch"); }
            let eps = 1e-6;
            let w: Vec<f64> = y.iter().zip(den.iter()).map(|(&p, &m)| {
                const MAX_WEIGHT: f64 = 1000.0; // cap to prevent overflow
                let p = p.clamp(eps, 1.0 - eps);
                let m = (m as f64).max(1.0);
                (m / (p * (1.0 - p))).min(MAX_WEIGHT) // <-- cap here
        }).collect();
            Ok(Some(w))
        }
    }
}
