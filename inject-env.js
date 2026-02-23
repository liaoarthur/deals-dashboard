// Build-time script: injects NEXT_PUBLIC_API_BASE_URL into dashboard.html
// Runs during Vercel build to replace the localhost default with the production API URL
const fs = require('fs');
const path = require('path');

const apiBaseUrl = process.env.NEXT_PUBLIC_API_BASE_URL || 'https://web-production-768f7.up.railway.app';
const htmlPath = path.join(__dirname, 'public', 'index.html');

let html = fs.readFileSync(htmlPath, 'utf8');
html = html.replace(
  "window.API_BASE_URL = window.API_BASE_URL || 'https://web-production-768f7.up.railway.app';",
  `window.API_BASE_URL = '${apiBaseUrl}';`
);
fs.writeFileSync(htmlPath, html, 'utf8');

console.log(`[inject-env] API_BASE_URL set to: ${apiBaseUrl}`);
