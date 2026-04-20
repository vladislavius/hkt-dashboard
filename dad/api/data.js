const fs = require('fs');
const path = require('path');

// DAD API endpoint — отдаёт dad/data/ (аналог /api/data.js для HKT, но свои пути).
module.exports = (req, res) => {
  const key = req.query.key || req.headers['x-api-key'] || '';
  if (!key || key !== process.env.API_SECRET) {
    res.status(401).json({ error: 'Unauthorized. Provide ?key=YOUR_KEY or X-API-Key header.' });
    return;
  }

  const date = req.query.date;
  const type = req.query.type || 'dashboard';

  let filePath;
  const baseDir = path.join(process.cwd(), 'dad', 'data');

  if (type === 'dashboard') {
    filePath = path.join(baseDir, 'dashboard.json');
  } else if (type === 'accumulated' && date && /^\d{4}-\d{2}-\d{2}$/.test(date)) {
    filePath = path.join(baseDir, `accumulated_${date}.json`);
  } else if (type === 'dates') {
    const files = fs.readdirSync(baseDir)
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
    res.status(404).json({ error: 'DAD data not found for this date.' });
    return;
  }

  const content = fs.readFileSync(filePath, 'utf8');
  res.setHeader('Content-Type', 'application/json');
  res.setHeader('Cache-Control', 'no-store');
  res.status(200).send(content);
};
