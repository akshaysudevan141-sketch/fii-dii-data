const axios = require('axios');
const fs = require('fs');
const path = require('path');
const { parse } = require('csv-parse/sync');
const sqlite3 = require('sqlite3');
const { open } = require('sqlite');

// ── Configuration ───────────────────────────────────────────────────────────
const CONFIG = {
    NSE_HOME: "https://www.nseindia.com/",
    NSE_API: "https://www.nseindia.com/api/fiidiiTradeReact",
    FAO_BASE: "https://nsearchives.nseindia.com/content/nsccl",
    TIMEOUTS: { cash: 25000, fao: 15000 },
    RETRY: { attempts: 3, baseDelayMs: 2000 },
    HISTORY_MAX: 60,
    DATA_DIR: path.join(process.cwd(), 'data'),
    DB_PATH: path.join(process.cwd(), 'data', 'fiidii.db')
};

const HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Referer": "https://www.nseindia.com/reports-indices-fii-dii-trading-activity"
};

let nseCookies = "";

// ── Database Connection ──────────────────────────────────────────────────────
async function getDB() {
    return open({
        filename: CONFIG.DB_PATH,
        driver: sqlite3.Database
    });
}

// ── Retry helper ─────────────────────────────────────────────────────────────
async function withRetry(fn, label) {
    let lastError;
    for (let attempt = 1; attempt <= CONFIG.RETRY.attempts; attempt++) {
        try {
            return await fn();
        } catch (err) {
            lastError = err;
            if (attempt < CONFIG.RETRY.attempts) {
                const delay = CONFIG.RETRY.baseDelayMs * attempt;
                console.warn(`  ⚠️  ${label} failed (attempt ${attempt}/${CONFIG.RETRY.attempts}): ${err.message}. Retrying in ${delay}ms…`);
                await new Promise(r => setTimeout(r, delay));
            }
        }
    }
    throw lastError;
}

// ── NSE Session Management ───────────────────────────────────────────────────
async function refreshNSESession() {
    try {
        const response = await axios.get(CONFIG.NSE_HOME, { headers: HEADERS, timeout: 10000 });
        const cookies = response.headers['set-cookie'];
        if (cookies) {
            nseCookies = cookies.map(c => c.split(';')[0]).join('; ');
            return true;
        }
    } catch (err) {
        console.error("  ❌ Failed to refresh NSE session:", err.message);
    }
    return false;
}

// ── Log Fetch Attempt to SQLite ──────────────────────────────────────────────
async function logFetch(entry) {
    try {
        const db = await getDB();
        await db.run(`
            INSERT INTO fetch_logs (ts, success, date, action, error, reason)
            VALUES (?, ?, ?, ?, ?, ?)
        `, [
            new Date().toISOString(),
            entry.success ? 1 : 0,
            entry.date || null,
            entry.action || null,
            entry.error || null,
            entry.reason || null
        ]);
        await db.close();
    } catch (err) {
        console.error("  ❌ Failed to log fetch to DB:", err.message);
    }
}

// ── Fetch cash data from NSE API ─────────────────────────────────────────────
async function fetchNSE() {
    if (!nseCookies) await refreshNSESession();

    return withRetry(async () => {
        try {
            const response = await axios.get(CONFIG.NSE_API, { 
                headers: { ...HEADERS, Cookie: nseCookies }, 
                timeout: CONFIG.TIMEOUTS.cash 
            });
            const data = response.data;
            if (Array.isArray(data) && data.length > 0) return data;
        } catch (err) {
            console.warn(`  ⚠️ Direct NSE fetch failed: ${err.message}. Trying proxy...`);
        }

        // Fallback to proxy
        const safeUrl = CONFIG.NSE_API.includes('?') ? `${CONFIG.NSE_API}&t=${Date.now()}` : `${CONFIG.NSE_API}?t=${Date.now()}`;
        const proxyUrl = `https://api.allorigins.win/get?url=${encodeURIComponent(safeUrl)}`;
        const pRes = await axios.get(proxyUrl, { timeout: CONFIG.TIMEOUTS.cash });
        const parsed = pRes.data && pRes.data.contents ? JSON.parse(pRes.data.contents) : null;
        if (Array.isArray(parsed) && parsed.length > 0) return parsed;

        throw new Error("NSE API & Proxy both returned empty or non-array response");
    }, "NSE cash API");
}

// ── Fetch F&O OI CSV with URL fallback ───────────────────────────────────────
async function fetchFaoOi(dateStr) {
    const MONTHS = { Jan:'01',Feb:'02',Mar:'03',Apr:'04',May:'05',Jun:'06',Jul:'07',Aug:'08',Sep:'09',Oct:'10',Nov:'11',Dec:'12' };
    const parts = dateStr.split('-');
    if (parts.length !== 3) return null;

    const day   = parts[0].padStart(2, '0');
    const month = MONTHS[parts[1]];
    const year  = parts[2];
    if (!month) return null;

    const datePart = `${day}${month}${year}`;
    const urls = [
        `${CONFIG.FAO_BASE}/fao_participant_oi_${datePart}_b.csv`,
        `${CONFIG.FAO_BASE}/fao_participant_oi_${datePart}.csv`,
    ];

    for (const url of urls) {
        try {
            const response = await withRetry(
                () => axios.get(url, { headers: { ...HEADERS, Cookie: nseCookies }, timeout: CONFIG.TIMEOUTS.fao }),
                `F&O CSV (${url})`
            );
            if (response.data && response.data.length > 0) return response.data;
        } catch { /* Try next */ }
    }
    return null;
}

// ── Parse F&O CSV ─────────────────────────────────────────────────────────────
function parseFao(csvText) {
    const faoData = {};
    if (!csvText) return faoData;

    try {
        const lines = csvText.trim().split('\n');
        if (lines.length < 2) return faoData;

        const records = parse(lines.slice(1).join('\n'), {
            skip_empty_lines: true,
            relax_column_count: true
        });

        const getInt = (val) => {
            if (!val) return 0;
            const n = parseInt(String(val).trim().replace(/,/g, ''), 10);
            return isNaN(n) ? 0 : n;
        };

        for (let i = 0; i < records.length; i++) {
            const row = records[i];
            if (!row || row.length < 9) continue;

            const clientType = (row[0] || "").trim().toUpperCase();
            if (!clientType.includes("FII") && !clientType.includes("DII")) continue;

            const key = clientType.includes("FII") ? "FII" : "DII";
            faoData[key] = {
                idx_fut_long:   getInt(row[1]),
                idx_fut_short:  getInt(row[2]),
                stk_fut_long:   getInt(row[3]),
                stk_fut_short:  getInt(row[4]),
                idx_call_long:  getInt(row[5]),
                idx_call_short: getInt(row[6]),
                idx_put_long:   getInt(row[7]),
                idx_put_short:  getInt(row[8]),
            };
        }
    } catch (e) {
        console.error("Error parsing F&O CSV:", e.message);
    }
    return faoData;
}

// ── Validate data ────────────────────────────────────────────────────────────
function validateData(data) {
    if (!data.date) return false;
    const fields = ['fii_buy','fii_sell','fii_net','dii_buy','dii_sell','dii_net'];
    return fields.every(f => isFinite(data[f]));
}

// ── Transform data ───────────────────────────────────────────────────────────
async function transformData(rawCash, rawFaoCsv) {
    const out = {
        date: "",
        fii_buy: 0, fii_sell: 0, fii_net: 0,
        dii_buy: 0, dii_sell: 0, dii_net: 0,
        fii_idx_fut_long: 0, fii_idx_fut_short: 0, fii_idx_fut_net: 0,
        dii_idx_fut_long: 0, dii_idx_fut_short: 0, dii_idx_fut_net: 0,
        fii_stk_fut_long: 0, fii_stk_fut_short: 0, fii_stk_fut_net: 0,
        dii_stk_fut_long: 0, dii_stk_fut_short: 0, dii_stk_fut_net: 0,
        fii_idx_call_long: 0, fii_idx_call_short: 0, fii_idx_call_net: 0,
        fii_idx_put_long: 0, fii_idx_put_short: 0, fii_idx_put_net: 0,
        pcr: 0,
        sentiment_score: 50, // Default neutral
    };

    for (const row of rawCash) {
        const cat = (row.category || "").toUpperCase();
        if (cat.includes("FII") || cat.includes("FPI")) {
            out.fii_buy = parseFloat(row.buyValue || 0);
            out.fii_sell = parseFloat(row.sellValue || 0);
            out.fii_net = parseFloat(row.netValue || 0);
            out.date = row.date || "";
        } else if (cat.includes("DII")) {
            out.dii_buy = parseFloat(row.buyValue || 0);
            out.dii_sell = parseFloat(row.sellValue || 0);
            out.dii_net = parseFloat(row.netValue || 0);
        }
    }

    if (out.date && rawFaoCsv) {
        const fao = parseFao(rawFaoCsv);
        if (fao["FII"]) {
            const f = fao["FII"];
            out.fii_idx_fut_long = f.idx_fut_long; out.fii_idx_fut_short = f.idx_fut_short; out.fii_idx_fut_net = f.idx_fut_long - f.idx_fut_short;
            out.fii_stk_fut_long = f.stk_fut_long; out.fii_stk_fut_short = f.stk_fut_short; out.fii_stk_fut_net = f.stk_fut_long - f.stk_fut_short;
            out.fii_idx_call_long = f.idx_call_long; out.fii_idx_call_short = f.idx_call_short; out.fii_idx_call_net = f.idx_call_long - f.idx_call_short;
            out.fii_idx_put_long = f.fii_idx_put_long; out.fii_idx_put_short = f.fii_idx_put_short; out.fii_idx_put_net = f.fii_idx_put_net;
            
            // --- PCR Calculation ---
            if (f.idx_call_short > 0) {
                out.pcr = parseFloat((f.idx_put_short / f.idx_call_short).toFixed(2));
            } else {
                out.pcr = 1.0;
            }

            // --- Sentiment Score (0-100) ---
            // Simple logic: Base 50. 
            // + Cash Net (1000Cr = +5 pts)
            // + Index Future Net (10000 contracts = +2 pts)
            // - Put/Call ratio high (>1.2 = Bearish, <0.8 = Bullish for short sellers/writers)
            // NSE PCR is usually calculated on OI. Here we use Participant OI.
            let sentiment = 50;
            sentiment += (out.fii_net / 200); // 1000Cr net buy = +5
            sentiment += (out.fii_idx_fut_net / 5000); // 10000 contracts = +2
            
            // PCR Sentiment (contrarian)
            if (out.pcr > 1.3) sentiment -= 10;
            if (out.pcr < 0.7) sentiment += 10;

            out.sentiment_score = Math.min(100, Math.max(0, parseFloat(sentiment.toFixed(1))));
        }
        if (fao["DII"]) {
            const d = fao["DII"];
            out.dii_idx_fut_long = d.idx_fut_long; out.dii_idx_fut_short = d.idx_fut_short; out.dii_idx_fut_net = d.idx_fut_long - d.idx_fut_short;
            out.dii_stk_fut_long = d.stk_fut_long; out.dii_stk_fut_short = d.stk_fut_short; out.dii_stk_fut_net = d.stk_fut_long - d.stk_fut_short;
        }
    }

    out._updated_at = new Date().toLocaleString("en-IN", { timeZone: "Asia/Kolkata", dateStyle: 'medium', timeStyle: 'short' }) + " IST";
    out._source = "fetch-pipeline";
    return out;
}

// ── Save to SQLite ──────────────────────────────────────────────────────────
async function saveToDB(data) {
    const db = await getDB();
    await db.run(`
        INSERT OR REPLACE INTO flows (
            date, fii_buy, fii_sell, fii_net, dii_buy, dii_sell, dii_net,
            fii_idx_fut_long, fii_idx_fut_short, fii_idx_fut_net,
            dii_idx_fut_long, dii_idx_fut_short, dii_idx_fut_net,
            fii_stk_fut_long, fii_stk_fut_short, fii_stk_fut_net,
            dii_stk_fut_long, dii_stk_fut_short, dii_stk_fut_net,
            fii_idx_call_long, fii_idx_call_short, fii_idx_call_net,
            fii_idx_put_long, fii_idx_put_short, fii_idx_put_net,
            pcr, sentiment_score,
            _updated_at, _source
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `, [
        data.date, data.fii_buy, data.fii_sell, data.fii_net, data.dii_buy, data.dii_sell, data.dii_net,
        data.fii_idx_fut_long, data.fii_idx_fut_short, data.fii_idx_fut_net,
        data.dii_idx_fut_long, data.dii_idx_fut_short, data.dii_idx_fut_net,
        data.fii_stk_fut_long, data.fii_stk_fut_short, data.fii_stk_fut_net,
        data.dii_stk_fut_long, data.dii_stk_fut_short, data.dii_stk_fut_net,
        data.fii_idx_call_long, data.fii_idx_call_short, data.fii_idx_call_net,
        data.fii_idx_put_long, data.fii_idx_put_short, data.fii_idx_put_net,
        data.pcr, data.sentiment_score,
        data._updated_at, data._source
    ]);
    await db.close();
}

// ── Atomic file write (Legacy Fallback) ──────────────────────────────────────
function writeFileAtomic(filePath, content) {
    const tmp = filePath + '.tmp';
    fs.writeFileSync(tmp, content, 'utf8');
    fs.renameSync(tmp, filePath);
}

// ── Pipeline ─────────────────────────────────────────────────────────────────
async function fetchAndProcessData() {
    const timestamp = new Date().toISOString();
    console.log(`[${timestamp}] Pipeline started…`);

    try {
        const rawCash = await fetchNSE();
        let targetDate = (rawCash.find(r => /FII|FPI|DII/i.test(r.category)) || {}).date;
        if (!targetDate) {
            console.log("ℹ️ No target date found in NSE response.");
            await logFetch({ success: true, action: "idle", reason: "no_data_date" });
            return null;
        }

        // Smart skip: check if we already have this exact data in DB
        const db = await getDB();
        const existing = await db.get(`SELECT * FROM flows WHERE date = ?`, [targetDate]);
        await db.close();

        if (existing && existing.fii_net !== 0) {
            console.log(`ℹ️ Data for ${targetDate} already exists in DB. Skipping store.`);
            await logFetch({ success: true, date: targetDate, action: "skipped" });
            // Update latest.json for legacy frontend compatibility
            writeFileAtomic(path.join(CONFIG.DATA_DIR, 'latest.json'), JSON.stringify(existing, null, 2));
            return { ...existing, _skipped: true };
        }

        const rawFaoCsv = await fetchFaoOi(targetDate);
        const data = await transformData(rawCash, rawFaoCsv);

        if (!validateData(data)) throw new Error(`Validation failed for ${data.date}`);

        await saveToDB(data);
        
        // Legacy file updates for backwards compatibility
        writeFileAtomic(path.join(CONFIG.DATA_DIR, 'latest.json'), JSON.stringify(data, null, 2));

        console.log(`✅ Updated: ${data.date} (FII Net: ${data.fii_net})`);
        await logFetch({ success: true, date: data.date, action: "updated" });
        return data;

    } catch (err) {
        console.error("❌ Pipeline error:", err.message);
        await logFetch({ success: false, error: err.message });
        throw err;
    }
}

if (require.main === module) {
    fetchAndProcessData().catch(() => process.exit(1));
}

module.exports = { fetchAndProcessData, getDB };
