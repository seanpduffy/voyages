// ═══════════════════════════════════════════════════════════════
// Voyages Climate Data Collector
// Runs via GitHub Actions every 6 hours.
// Each run collects ~1 month of data, staying within rate limits.
// Progress is saved to climate-grid.json between runs.
// ═══════════════════════════════════════════════════════════════

const fs = require('fs');
const path = require('path');

const GRID_STEP = 2;
const BATCH_SIZE = 400;
const DATA_YEAR = 2024;
const MAX_LOCATIONS_PER_RUN = 4800; // Stay under 5,000/hour
const OUTPUT_FILE = path.join(__dirname, '..', 'climate-grid.json');
const DELAY_MS = 600; // ms between batches

const DAILY_VARS = [
  'temperature_2m_max',
  'temperature_2m_min',
  'precipitation_sum',
  'snowfall_sum',
  'sunshine_duration',
  'wind_speed_10m_max',
  'cloud_cover_mean',
];

// ── Land detection ──
function isLand(lat, lng) {
  if (lng >= -170 && lng <= -50 && lat >= 15 && lat <= 72) {
    if (lng >= -140 && lng <= -60 && lat >= 25 && lat <= 70) return true;
    if (lng >= -170 && lng <= -140 && lat >= 55 && lat <= 72) return true;
    if (lng >= -120 && lng <= -80 && lat >= 15 && lat <= 32) return true;
    if (lng >= -90 && lng <= -60 && lat >= 8 && lat <= 20) return true;
  }
  if (lng >= -55 && lng <= -20 && lat >= 60 && lat <= 83) return true;
  if (lng >= -82 && lng <= -34 && lat >= -56 && lat <= 12) {
    const r = (lat + 56) / 68, c = -70 + r * 30, w = 18 - Math.abs(r - 0.45) * 10;
    if (Math.abs(lng - c) < w) return true;
  }
  if (lng >= -10 && lng <= 60 && lat >= 36 && lat <= 72) return true;
  if (lng >= -25 && lng <= -5 && lat >= 36 && lat <= 44) return true;
  if (lng >= -12 && lng <= 2 && lat >= 50 && lat <= 60) return true;
  if (lng >= -18 && lng <= 52 && lat >= -35 && lat <= 37) {
    const r = (lat + 35) / 72, c = 20 - (r - 0.5) * 8, w = 25 - Math.abs(r - 0.5) * 15;
    if (Math.abs(lng - c) < w) return true;
  }
  if (lng >= 43 && lng <= 51 && lat >= -26 && lat <= -12) return true;
  if (lng >= 25 && lng <= 180 && lat >= 0 && lat <= 78) {
    if (lng >= 60 && lng <= 150 && lat >= 10 && lat <= 70) return true;
    if (lng >= 25 && lng <= 60 && lat >= 12 && lat <= 45) return true;
    if (lng >= 92 && lng <= 106 && lat >= 8 && lat <= 22) return true;
    if (lng >= 95 && lng <= 141 && lat >= -11 && lat <= 6) return true;
    if (lng >= 118 && lng <= 127 && lat >= 5 && lat <= 19) return true;
    if (lng >= 130 && lng <= 146 && lat >= 30 && lat <= 46) return true;
  }
  if (lng >= 112 && lng <= 154 && lat >= -40 && lat <= -10) return true;
  if (lng >= 166 && lng <= 179 && lat >= -47 && lat <= -34) return true;
  return false;
}

// ── Build grid ──
const gridPoints = [];
for (let lat = -60; lat <= 70; lat += GRID_STEP) {
  for (let lng = -180; lng <= 178; lng += GRID_STEP) {
    if (isLand(lat, lng)) gridPoints.push({ lat, lng });
  }
}

// ── Load existing progress ──
function loadProgress() {
  try {
    if (fs.existsSync(OUTPUT_FILE)) {
      const data = JSON.parse(fs.readFileSync(OUTPUT_FILE, 'utf8'));
      if (data.points && data.monthsDone !== undefined) {
        return { climateData: data.points, monthsDone: data.monthsDone };
      }
    }
  } catch (e) {
    console.log('No existing progress found, starting fresh');
  }
  return { climateData: {}, monthsDone: 0 };
}

// ── Save progress ──
function saveProgress(climateData, monthsDone) {
  const output = {
    version: 1,
    gridStep: GRID_STEP,
    dataYear: DATA_YEAR,
    monthsDone: monthsDone,
    generatedAt: new Date().toISOString(),
    variables: ['tHi', 'tLo', 'rain', 'snow', 'sun', 'wind', 'cloud'],
    variableDescriptions: {
      tHi: 'Avg daily high (°F)',
      tLo: 'Avg daily low (°F)',
      rain: '% days with precipitation',
      snow: '% days with snowfall',
      sun: 'Avg sunshine hours/day',
      wind: 'Avg max wind speed (mph)',
      cloud: 'Avg cloud cover %',
    },
    points: climateData,
  };
  fs.writeFileSync(OUTPUT_FILE, JSON.stringify(output));
  console.log(`Saved: ${monthsDone}/12 months, ${Object.keys(climateData).length} points`);
}

// ── Fetch a batch from Open-Meteo ──
async function fetchBatch(points, month, activeVars) {
  const lats = points.map(p => p.lat).join(',');
  const lngs = points.map(p => p.lng).join(',');
  const ms = String(month).padStart(2, '0');
  const dim = new Date(DATA_YEAR, month, 0).getDate();
  const url = `https://archive-api.open-meteo.com/v1/archive`
    + `?latitude=${lats}&longitude=${lngs}`
    + `&start_date=${DATA_YEAR}-${ms}-01`
    + `&end_date=${DATA_YEAR}-${ms}-${String(dim).padStart(2, '0')}`
    + `&daily=${activeVars.join(',')}`
    + `&temperature_unit=fahrenheit&precipitation_unit=inch&timezone=auto`;

  const resp = await fetch(url);
  if (!resp.ok) {
    const text = await resp.text().catch(() => '');
    throw new Error(`HTTP ${resp.status}: ${text.substring(0, 300)}`);
  }
  return resp.json();
}

// ── Process API results ──
function processResults(data, points, month, climateData) {
  const results = Array.isArray(data) ? data : [data];
  const avg = (arr) => {
    if (!arr) return null;
    const v = arr.filter(x => x != null);
    return v.length ? Math.round(v.reduce((s, x) => s + x, 0) / v.length * 10) / 10 : null;
  };
  const pctDays = (arr, thresh) => {
    if (!arr) return null;
    const v = arr.filter(x => x != null);
    if (!v.length) return null;
    return Math.round(v.filter(x => x > thresh).length / v.length * 100);
  };

  for (let i = 0; i < points.length; i++) {
    const pt = points[i], key = `${pt.lat},${pt.lng}`;
    const r = results[i];
    if (!r || !r.daily || !r.daily.time || !r.daily.time.length) continue;
    const d = r.daily;
    if (!climateData[key]) climateData[key] = {};
    climateData[key][month] = {
      tHi: avg(d.temperature_2m_max),
      tLo: avg(d.temperature_2m_min),
      rain: pctDays(d.precipitation_sum, 0.02),
      snow: pctDays(d.snowfall_sum, 0),
      sun: d.sunshine_duration ? Math.round(avg(d.sunshine_duration) / 3600) : null,
      wind: avg(d.wind_speed_10m_max),
      cloud: d.cloud_cover_mean ? avg(d.cloud_cover_mean) : null,
    };
  }
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ── Main ──
async function main() {
  console.log(`Grid: ${GRID_STEP}° — ${gridPoints.length} land points`);

  let { climateData, monthsDone } = loadProgress();
  console.log(`Progress: ${monthsDone}/12 months done`);

  if (monthsDone >= 12) {
    console.log('All 12 months already collected. Nothing to do.');
    process.exit(0);
  }

  // Save initial state so there's always a file for git to commit
  saveProgress(climateData, monthsDone);

  let activeVars = [...DAILY_VARS];
  let locationCallsThisRun = 0;
  const months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];

  // Process one month at a time until we hit our per-run budget
  for (let month = monthsDone + 1; month <= 12; month++) {
    console.log(`\n── ${months[month - 1]} (month ${month}/12) ──`);

    // Check if this month would exceed our per-run budget
    if (locationCallsThisRun + gridPoints.length > MAX_LOCATIONS_PER_RUN) {
      console.log(`Budget: ${locationCallsThisRun} calls used this run. Saving for next run.`);
      break;
    }

    let monthSuccess = true;
    const totalBatches = Math.ceil(gridPoints.length / BATCH_SIZE);

    for (let bStart = 0; bStart < gridPoints.length; bStart += BATCH_SIZE) {
      const batch = gridPoints.slice(bStart, bStart + BATCH_SIZE);
      const batchNum = Math.floor(bStart / BATCH_SIZE) + 1;

      try {
        const data = await fetchBatch(batch, month, activeVars);
        processResults(data, batch, month, climateData);
        locationCallsThisRun += batch.length;
        console.log(`  Batch ${batchNum}/${totalBatches}: ${batch.length} points ✓ (${locationCallsThisRun} total calls)`);
      } catch (e) {
        // If cloud_cover_mean isn't supported, drop it and retry
        if (e.message.includes('400') && e.message.includes('WeatherVariable') && activeVars.includes('cloud_cover_mean')) {
          console.log('  Dropping cloud_cover_mean (unsupported by archive API)');
          activeVars = activeVars.filter(v => v !== 'cloud_cover_mean');
          bStart -= BATCH_SIZE; // Retry this batch
          continue;
        }

        // If rate limited, save and exit gracefully
        if (e.message.includes('429')) {
          console.log(`  Rate limited. Saving progress and exiting.`);
          saveProgress(climateData, monthsDone);
          process.exit(0);
        }

        console.log(`  Batch ${batchNum} ERROR: ${e.message}`);
        monthSuccess = false;
      }

      await sleep(DELAY_MS);
    }

    if (monthSuccess) {
      monthsDone = month;
      saveProgress(climateData, monthsDone);
      console.log(`✓ ${months[month - 1]} complete (${monthsDone}/12)`);
    }
  }

  if (monthsDone >= 12) {
    console.log('\n✓ ALL 12 MONTHS COMPLETE! climate-grid.json is ready.');
  } else {
    console.log(`\nRun complete. ${monthsDone}/12 months done. Next run will continue.`);
  }
}

main().catch(e => {
  console.error('Fatal error:', e);
  process.exit(1);
});
