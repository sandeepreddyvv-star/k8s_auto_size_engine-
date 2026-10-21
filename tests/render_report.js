// Renders out/report.html's script against a minimal DOM stub and prints the generated HTML.
// Used by tests/test_report.py to catch template errors without a browser. Usage: node render_report.js report.html
const fs = require('fs');
const html = fs.readFileSync(process.argv[2], 'utf8');
const script = html.slice(html.lastIndexOf('<script>') + 8, html.lastIndexOf('</script>'));
const app = { innerHTML: '', querySelectorAll: () => [], querySelector: () => null };
global.document = { title: '', querySelector: s => (s === '#app' ? app : null) };
global.localStorage = { getItem: () => null, setItem: () => {} };
new Function(script)();
process.stdout.write(JSON.stringify({ title: document.title, html: app.innerHTML }));
