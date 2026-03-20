const express = require('express');
const cors = require('cors');
const compression = require('compression');
const path = require('path');
const fs = require('fs');
const http = require('http');
const { Server } = require('socket.io');
const axios = require('axios');
const cron = require('node-cron');
const { fetchAndProcessData, getDB } = require('./scripts/fetch_data');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
    cors: {
        origin: "*",
        methods: ["GET", "POST"]
    }
});

const PORT = process.env.PORT || 5000;
const DATA_DIR = path.join(process.cwd(), 'data');

// ── State for Monitoring ─────────────────────────────────────────────────────
const fetchStatus = {
    lastAttemptAt: null,
    lastSuccessAt: null,
    lastSuccessDate: null,
    consecutiveFailures: 0,
    fetchCount: 0
};

// ── Middleware ────────────────────────────────────────────────────────────────
app.use(cors());
app.use(compression()); // Gzip all responses

// Security Headers
app.use((req, res, next) => {
    res.setHeader('X-Content-Type-Options', 'nosniff');
    res.setHeader('X-Frame-Options', 'SAMEORIGIN');
    res.setHeader('X-XSS-Protection', '1; mode=block');
    res.setHeader('Referrer-Policy', 'strict-origin-when-cross-origin');
    res.setHeader('Permissions-Policy', 'camera=(), microphone=(), geolocation=()');
    if (process.env.NODE_ENV === 'production') {
        res.setHeader('Strict-Transport-Security', 'max-age=31536000; includeSubDomains');
    }
    next();
});

// Static assets with cache-control
app.use('/icons', express.static(path.join(__dirname, 'icons'), { maxAge: '30d', immutable: true }));
app.use('/screenshots', express.static(path.join(__dirname, 'screenshots'), { maxAge: '7d' }));
app.use(express.static(path.join(__dirname, '.'), {
    maxAge: process.env.NODE_ENV === 'production' ? '1h' : '0',
    setHeaders: (res, filePath) => {
        // manifest and SW should not be heavily cached
        if (filePath.endsWith('sw.js') || filePath.endsWith('manifest.json')) {
            res.setHeader('Cache-Control', 'no-cache');
        }
    }
}));
app.use(express.json());

// ── WebSockets ────────────────────────────────────────────────────────────────
io.on('connection', (socket) => {
    console.log(`🔌 Client connected: ${socket.id}`);
    socket.on('disconnect', () => console.log(`🔌 Client disconnected: ${socket.id}`));
});

// ── Task: Run Pipeline ────────────────────────────────────────────────────────
async function runFetchTask(label = "Scheduled") {
    fetchStatus.lastAttemptAt = new Date().toISOString();
    console.log(`[${new Date().toISOString()}] 📡 ${label} fetch starting…`);
    try {
        const result = await fetchAndProcessData();
        fetchStatus.lastSuccessAt = new Date().toISOString();
        fetchStatus.fetchCount++;
        fetchStatus.consecutiveFailures = 0;
        if (result && result.date) {
            fetchStatus.lastSuccessDate = result.date;
            // Emit real-time update
            io.emit('data_updated', result);
        }
        console.log(`[${new Date().toISOString()}] ✅ ${label} fetch completed.`);
        return result;
    } catch (err) {
        fetchStatus.consecutiveFailures++;
        console.error(`[${new Date().toISOString()}] ❌ ${label} fetch failed:`, err.message);
        throw err;
    }
}

// ── Routes ────────────────────────────────────────────────────────────────────

// Dashboard
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'fii_dii_india_flows_dashboard.html'));
});

// Yahoo Finance proxy for Nifty and VIX (Bypasses browser CORS)
app.get('/api/market', async (req, res) => {
    try {
        const fetchJSON = async (ticker) => {
            const url = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(ticker)}?interval=1d&range=1d&includePrePost=false`;
            const { data } = await axios.get(url, { headers: { "User-Agent": "Mozilla/5.0" }, timeout: 8000 });
            const m = data.chart.result[0].meta;
            const price = m.regularMarketPrice;
            const prev = m.previousClose || m.chartPreviousClose;
            return { price, change: price - prev, pct: ((price - prev) / prev) * 100, state: m.marketState };
        };
        const [nifty, vix] = await Promise.all([fetchJSON('^NSEI'), fetchJSON('^INDIAVIX')]);
        res.json({ nifty, vix });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// Latest FII/DII snapshot (from DB)
app.get('/api/data', async (req, res) => {
    try {
        const db = await getDB();
        const data = await db.all('SELECT * FROM flows ORDER BY rowid DESC LIMIT 10');
        await db.close();
        if (!data || data.length === 0) return res.status(404).json({ error: 'No data found.' });
        
        const M = {Jan:0,Feb:1,Mar:2,Apr:3,May:4,Jun:5,Jul:6,Aug:7,Sep:8,Oct:9,Nov:10,Dec:11};
        data.sort((a,b) => {
            if(!a.date || !b.date) return 0;
            let pa = a.date.split('-'); let da = new Date(pa[2], M[pa[1]], pa[0]).getTime();
            let pb = b.date.split('-'); let db = new Date(pb[2], M[pb[1]], pb[0]).getTime();
            return db - da;
        });

        res.json(data[0]);
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// Rolling history (from DB)
app.get('/api/history', async (req, res) => {
    try {
        const db = await getDB();
        const history = await db.all('SELECT * FROM flows ORDER BY rowid DESC LIMIT 100');
        await db.close();
        
        const M = {Jan:0,Feb:1,Mar:2,Apr:3,May:4,Jun:5,Jul:6,Aug:7,Sep:8,Oct:9,Nov:10,Dec:11};
        history.sort((a,b) => {
            if(!a.date || !b.date) return 0;
            let pa = a.date.split('-'); let da = new Date(pa[2], M[pa[1]], pa[0]).getTime();
            let pb = b.date.split('-'); let db = new Date(pb[2], M[pb[1]], pb[0]).getTime();
            return db - da;
        });

        res.json(history.slice(0, 60));
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// Pipeline status
app.get('/api/status', async (req, res) => {
    try {
        const db = await getDB();
        const lastLogs = await db.all('SELECT * FROM fetch_logs ORDER BY id DESC LIMIT 5');
        await db.close();
        res.json({
            ...fetchStatus,
            serverTime: new Date().toISOString(),
            recentLogs: lastLogs
        });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// Manual trigger
app.post('/api/refresh', async (req, res) => {
    try {
        const data = await runFetchTask("Manual");
        res.json({ success: true, data });
    } catch (err) {
        res.status(500).json({ success: false, error: err.message });
    }
});

// Health check
app.get('/health', (req, res) => {
    res.json({ status: 'ok', ts: new Date().toISOString() });
});

// 404 Catch-All (must be last route)
app.use((req, res) => {
    // Redirect unknown routes to dashboard
    res.status(404).redirect('/');
});

// ── Scheduler ─────────────────────────────────────────────────────────────────

// Mon-Fri: Every 15 mins during market hours (9:15 AM - 3:30 PM IST)
cron.schedule('*/15 3-10 * * 1-5', () => runFetchTask("Cron Intra-day"));

// Extra runs for final data (15:45 IST, 18:00 IST)
cron.schedule('45 10 * * 1-5', () => runFetchTask("Cron Post-market-1"));
cron.schedule('0 12 * * 1-5', () => runFetchTask("Cron Post-market-2"));

// ── Start ─────────────────────────────────────────────────────────────────────
server.listen(PORT, async () => {
    console.log(`🚀 Server running on http://localhost:${PORT}`);
    
    // Startup Backfill: Check if stale
    try {
        const db = await getDB();
        const latest = await db.get('SELECT date FROM flows ORDER BY rowid DESC LIMIT 1');
        await db.close();

        const todayStr = new Date().toLocaleString('en-IN', { day:'2-digit', month:'short', year:'numeric', timeZone:'Asia/Kolkata' }).replace(/ /g, '-');
        let needsFetch = !latest || (latest.date !== todayStr && new Date().getUTCHours() >= 4);

        if (needsFetch) {
            console.log("ℹ️ Startup backfill triggered...");
            await runFetchTask("Startup");
        }
    } catch (err) {
        console.error("⚠️ Startup backfill check failed:", err.message);
    }
});

module.exports = app;
