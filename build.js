import fs from 'fs';
import path from 'path';
import { execSync } from 'child_process';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

console.log('[Build] Compiling TypeScript with tsc...');
execSync('tsc', { stdio: 'inherit' });

// Synchronize static assets into public/ as a fail-safe for Vercel/Netlify static presets
const publicDir = path.join(__dirname, 'public');
const publicDistDir = path.join(publicDir, 'dist');

if (!fs.existsSync(publicDir)) {
  fs.mkdirSync(publicDir, { recursive: true });
}
if (!fs.existsSync(publicDistDir)) {
  fs.mkdirSync(publicDistDir, { recursive: true });
}

// Copy index.html
fs.copyFileSync(
  path.join(__dirname, 'index.html'),
  path.join(publicDir, 'index.html')
);

// Copy compiled JS and sourcemap files from dist to public/dist
const distDir = path.join(__dirname, 'dist');
if (fs.existsSync(distDir)) {
  const files = fs.readdirSync(distDir);
  for (const file of files) {
    fs.copyFileSync(
      path.join(distDir, file),
      path.join(publicDistDir, file)
    );
  }
}

console.log('[Build] Successfully generated build output in both root and public/ directories.');
