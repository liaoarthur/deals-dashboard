// Build-time script: injects NEXT_PUBLIC_API_BASE_URL into HTML files
// Runs during Vercel build to replace the Railway default with the production API URL
// Location: packages/lookalike-prospecting/static/inject-env.js
const fs = require('fs');
const path = require('path');

const apiBaseUrl = process.env.NEXT_PUBLIC_API_BASE_URL || 'https://web-production-768f7.up.railway.app';

// Inject into index.html (dashboard) — same directory as this script
const indexPath = path.join(__dirname, 'index.html');
if (fs.existsSync(indexPath)) {
    let indexHtml = fs.readFileSync(indexPath, 'utf8');
    indexHtml = indexHtml.replace(
        "window.API_BASE_URL = 'https://web-production-768f7.up.railway.app';",
        `window.API_BASE_URL = '${apiBaseUrl}';`
    );
    fs.writeFileSync(indexPath, indexHtml, 'utf8');
    console.log(`[inject-env] index.html API_BASE_URL set to: ${apiBaseUrl}`);
}

// Inject into login.html — same directory as this script
const loginPath = path.join(__dirname, 'login.html');
if (fs.existsSync(loginPath)) {
    let loginHtml = fs.readFileSync(loginPath, 'utf8');
    loginHtml = loginHtml.replace(
        "window.API_BASE_URL || 'https://web-production-768f7.up.railway.app'",
        `'${apiBaseUrl}'`
    );
    fs.writeFileSync(loginPath, loginHtml, 'utf8');
    console.log(`[inject-env] login.html API_BASE_URL set to: ${apiBaseUrl}`);
}

console.log(`[inject-env] Done. API_BASE_URL = ${apiBaseUrl}`);
