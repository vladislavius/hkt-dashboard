const fs = require('fs');
const path = require('path');

module.exports = (req, res) => {
  // Check API key
  const key = req.query.key || req.headers['x-api-key'] || '';
  if (!key || key !== process.env.API_SECRET) {
    res.status(401).json({ error: 'Unauthorized. Provide ?key=YOUR_KEY or X-API-Key header.' });
    return;
  }

  // Determine which file to serve
  const date = req.query.date;  // e.g. "2026-04-07"
  const type = req.query.type || 'dashboard'; // "dashboard" or "accumulated"

  let filePath;
  if (type === 'dashboard') {
    filePath = path.join(process.cwd(), 'data', 'dashboard.json');
  } else if (type === 'accumulated' && date && /^\d{4}-\d{2}-\d{2}$/.test(date)) {
    filePath = path.join(process.cwd(), 'data', `accumulated_${date}.json`);
  } else if (type === 'dates') {
    // Return list of available dates
    const dataDir = path.join(process.cwd(), 'data');
    const files = fs.readdirSync(dataDir)
      .filter(f => f.startsWith('accumulated_') && f.endsWith('.json'))
      .map(f => f.replace('accumulated_', '').replace('.json', ''))
      .sort();
    res.setHeader('Content-Type', 'application/json');
    res.setHeader('Cache-Control', 'no-store');
    res.status(200).json({ dates: files, count: files.length });
    return;
  } else {
    res.status(400).json({ error: 'Invalid params. Use ?type=dashboard or ?type=accumulated&date=YYYY-MM-DD or ?type=dates' });
    return;
  }

  if (!fs.existsSync(filePath)) {
    res.status(404).json({ error: 'Data not found for this date.' });
    return;
  }

  const content = fs.readFileSync(filePath, 'utf8');
  res.setHeader('Content-Type', 'application/json');
  res.setHeader('Cache-Control', 'no-store');
  res.status(200).send(content);
};
