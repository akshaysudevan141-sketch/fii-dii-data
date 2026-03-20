const sqlite3 = require('sqlite3');
const { open } = require('sqlite');
const fs = require('fs');
const path = require('path');

const DB_DIR = path.join(process.cwd(), 'data');
const DB_PATH = path.join(DB_DIR, 'fiidii.db');

async function initDB() {
    console.log(`🗄️ Initializing SQLite Database at ${DB_PATH}...`);
    
    if (!fs.existsSync(DB_DIR)) {
        fs.mkdirSync(DB_DIR, { recursive: true });
    }

    const db = await open({
        filename: DB_PATH,
        driver: sqlite3.Database
    });

    // Create Flows Table
    await db.exec(`
        CREATE TABLE IF NOT EXISTS flows (
            date TEXT PRIMARY KEY,
            fii_buy REAL,
            fii_sell REAL,
            fii_net REAL,
            dii_buy REAL,
            dii_sell REAL,
            dii_net REAL,
            fii_idx_fut_long INTEGER,
            fii_idx_fut_short INTEGER,
            fii_idx_fut_net INTEGER,
            dii_idx_fut_long INTEGER,
            dii_idx_fut_short INTEGER,
            dii_idx_fut_net INTEGER,
            fii_stk_fut_long INTEGER,
            fii_stk_fut_short INTEGER,
            fii_stk_fut_net INTEGER,
            dii_stk_fut_long INTEGER,
            dii_stk_fut_short INTEGER,
            dii_stk_fut_net INTEGER,
            fii_idx_call_long INTEGER,
            fii_idx_call_short INTEGER,
            fii_idx_call_net INTEGER,
            fii_idx_put_long INTEGER,
            fii_idx_put_short INTEGER,
            fii_idx_put_net INTEGER,
            _updated_at TEXT,
            _source TEXT
        )
    `);

    // Create Fetch Logs Table
    await db.exec(`
        CREATE TABLE IF NOT EXISTS fetch_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT,
            success INTEGER,
            date TEXT,
            action TEXT,
            error TEXT,
            reason TEXT
        )
    `);

    // Schema Migrations (Ensure new columns exist)
    const columns = await db.all("PRAGMA table_info(flows)");
    const columnNames = columns.map(c => c.name);

    if (!columnNames.includes('pcr')) {
        console.log("➕ Adding pcr column...");
        await db.exec("ALTER TABLE flows ADD COLUMN pcr REAL DEFAULT 0");
    }
    if (!columnNames.includes('sentiment_score')) {
        console.log("➕ Adding sentiment_score column...");
        await db.exec("ALTER TABLE flows ADD COLUMN sentiment_score REAL DEFAULT 50");
    }

    console.log("✅ Schema initialized.");
    return db;
}

async function migrateData() {
    const db = await initDB();
    
    // Migrate history.json
    const historyPath = path.join(DB_DIR, 'history.json');
    if (fs.existsSync(historyPath)) {
        console.log("🚚 Migrating history.json...");
        const history = JSON.parse(fs.readFileSync(historyPath, 'utf8'));
        
        for (const row of history) {
            await db.run(`
                INSERT OR REPLACE INTO flows (
                    date, fii_buy, fii_sell, fii_net, dii_buy, dii_sell, dii_net,
                    fii_idx_fut_long, fii_idx_fut_short, fii_idx_fut_net,
                    dii_idx_fut_long, dii_idx_fut_short, dii_idx_fut_net,
                    fii_stk_fut_long, fii_stk_fut_short, fii_stk_fut_net,
                    dii_stk_fut_long, dii_stk_fut_short, dii_stk_fut_net,
                    fii_idx_call_long, fii_idx_call_short, fii_idx_call_net,
                    fii_idx_put_long, fii_idx_put_short, fii_idx_put_net,
                    _updated_at, _source
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            `, [
                row.date, row.fii_buy, row.fii_sell, row.fii_net, row.dii_buy, row.dii_sell, row.dii_net,
                row.fii_idx_fut_long || 0, row.fii_idx_fut_short || 0, row.fii_idx_fut_net || 0,
                row.dii_idx_fut_long || 0, row.dii_idx_fut_short || 0, row.dii_idx_fut_net || 0,
                row.fii_stk_fut_long || 0, row.fii_stk_fut_short || 0, row.fii_stk_fut_net || 0,
                row.dii_stk_fut_long || 0, row.dii_stk_fut_short || 0, row.dii_stk_fut_net || 0,
                row.fii_idx_call_long || 0, row.fii_idx_call_short || 0, row.fii_idx_call_net || 0,
                row.fii_idx_put_long || 0, row.fii_idx_put_short || 0, row.fii_idx_put_net || 0,
                row._updated_at, row._source
            ]);
        }
        console.log(`✅ Migrated ${history.length} flow records.`);
    }

    // Migrate fetch-log.json
    const logPath = path.join(DB_DIR, 'fetch-log.json');
    if (fs.existsSync(logPath)) {
        console.log("🚚 Migrating fetch-log.json...");
        const logs = JSON.parse(fs.readFileSync(logPath, 'utf8'));
        
        for (const log of logs) {
            await db.run(`
                INSERT INTO fetch_logs (ts, success, date, action, error, reason)
                VALUES (?, ?, ?, ?, ?, ?)
            `, [
                log.ts, log.success ? 1 : 0, log.date, log.action, log.error, log.reason
            ]);
        }
        console.log(`✅ Migrated ${logs.length} log entries.`);
    }

    await db.close();
    console.log("🏁 Migration complete.");
}

migrateData().catch(console.error);
