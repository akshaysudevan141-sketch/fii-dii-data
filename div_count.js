const fs = require('fs');
const html = fs.readFileSync('fii_dii_india_flows_dashboard.html', 'utf8');
const lines = html.split('\n');
let depth = 0;
lines.forEach((l, i) => {
    const op = l.match(/<div/gi);
    const cl = l.match(/<\/div>/gi);
    if (op) depth += op.length;
    if (cl) depth -= cl.length;
    if (l.includes('id="t-hero"')) console.log('OPEN t-hero at', i+1, 'Depth After:', depth);
    if (l.includes('id="t-fno"')) console.log('OPEN t-fno at', i+1, 'Depth After:', depth);
    if (l.includes('id="t-matrix"')) console.log('OPEN t-matrix at', i+1, 'Depth After:', depth);
    if (l.includes('id="t-charts"')) console.log('OPEN t-charts at', i+1, 'Depth After:', depth);
    if (l.includes('id="t-docs"')) console.log('OPEN t-docs at', i+1, 'Depth After:', depth);
    if (l.includes('id="t-sector"')) console.log('OPEN t-sector at', i+1, 'Depth After:', depth);
});
console.log('Final depth:', depth);
