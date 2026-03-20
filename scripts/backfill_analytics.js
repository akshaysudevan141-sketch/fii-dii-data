const sqlite3 = require('sqlite3');
const { open } = require('sqlite');
const path = require('path');

const DB_PATH = path.join(process.cwd(), 'data', 'fiidii.db');

async function backfillAnalytics() {
    console.log("📊 Starting Analytics Backfill...");
    
    const db = await open({
        filename: DB_PATH,
        driver: sqlite3.Database
    });

    const rows = await db.all('SELECT * FROM flows');
    console.log(`🧐 Found ${rows.length} records to process.`);

    for (const row of rows) {
        // PCR Calculation
        let pcr = 1.0;
        if (row.fii_idx_call_short > 0) {
            pcr = parseFloat((row.fii_idx_put_short / row.fii_idx_call_short).toFixed(2));
        }

        // Sentiment Score Logic (Sync with fetch_data.js)
        let sentiment = 50;
        sentiment += (row.fii_net / 200); 
        sentiment += (row.fii_idx_fut_net / 5000);
        
        if (pcr > 1.3) sentiment -= 10;
        if (pcr < 0.7) sentiment += 10;

        const sentiment_score = Math.min(100, Math.max(0, parseFloat(sentiment.toFixed(1))));

        await db.run(`
            UPDATE flows 
            SET pcr = ?, sentiment_score = ?
            WHERE date = ?
        `, [pcr, sentiment_score, row.date]);
    }

    await db.close();
    console.log("✅ Backfill complete.");
}

backfillAnalytics().catch(console.error);
