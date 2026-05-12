/**
 * Standalone test for UI filtering logic in web/js/app.js
 */
const fs = require('fs');
const path = require('path');

// 1. Mock the environment
global.state = {
  activeSourceMode: 'all'
};

// Mock sets from symbols.js
global.ALL_ETFS = new Set(['SPY', 'QQQ']);
global.MAJOR_TARGETS = new Set(['AAPL', 'MSFT', 'SPY']);
global.TOP_AUM = new Set(['SPY', 'IVV']);
global.TOP_VOL = new Set(['TSLA', 'AMD']);
global.SPDR = new Set(['XLE', 'XLK']);
global.SPDR_SECTORS = new Set(['XLK', 'XLY']);
global.VANGUARD = new Set(['VOO', 'VTI']);
global.VANECK_ETFS = new Set(['GDX', 'GDXJ']);
global.COMMODITY_ETFS = new Set(['GLD', 'SLV']);
global.SECTOR_ETFS = new Set(['XBI', 'IBB']);

// 2. Load the logic from app.js using line numbers for robustness
const appJsLines = fs.readFileSync(path.join(__dirname, '../web/js/app.js'), 'utf8').split('\n');

// SOURCE_DEFS: Lines 4 to 17 (0-indexed: 3 to 17)
const sourceDefs = appJsLines.slice(3, 17).join('\n');
// matchesSources: Lines 198 to 207 (0-indexed: 197 to 207)
const matchesSourcesFunc = appJsLines.slice(197, 207).join('\n');

// Combine into one evaluation and use var to ensure they hit the global object in this context
const combinedCode = `
  var SOURCE_DEFS = ${sourceDefs.replace('const SOURCE_DEFS = ', '')};
  var matchesSources = ${matchesSourcesFunc};
  global.SOURCE_DEFS = SOURCE_DEFS;
  global.matchesSources = matchesSources;
`;

try {
  eval(combinedCode);
} catch (e) {
  console.error("Evaluation failed:");
  console.error(e);
  process.exit(1);
}

// 3. Test Runner
const assertions = [];
function assert(condition, message) {
  if (condition) {
    assertions.push({ status: 'PASS', message });
  } else {
    assertions.push({ status: 'FAIL', message });
  }
}

function runTests() {
  console.log("Running UI Filter Tests...\n");

  // --- Test 'all' filter ---
  state.activeSourceMode = 'all';
  assert(matchesSources('AAPL', { marketCap: 3e12 }) === true, "'all' filter matches Mega Cap");
  assert(matchesSources('SPY', { marketCap: 0 }) === true, "'all' filter matches ETF");

  // --- Test 'none' filter ---
  state.activeSourceMode = 'none';
  assert(matchesSources('AAPL', { marketCap: 3e12 }) === false, "'none' filter rejects Mega Cap");
  assert(matchesSources('SPY', { marketCap: 0 }) === false, "'none' filter rejects ETF");

  // --- Test 'small' filter ($200B-) ---
  state.activeSourceMode = new Set(['small']);
  assert(matchesSources('MID', { marketCap: 50e9 }) === true, "'small' filter matches $50B stock");
  assert(matchesSources('MEGA', { marketCap: 300e9 }) === false, "'small' filter rejects $300B stock");
  assert(matchesSources('ETF', { marketCap: 0 }) === false, "'small' filter rejects symbol with 0 marketCap (ETF)");

  // --- Test 'mega' filter ($200B+) ---
  state.activeSourceMode = new Set(['mega']);
  assert(matchesSources('MEGA', { marketCap: 300e9 }) === true, "'mega' filter matches $300B stock");
  assert(matchesSources('MID', { marketCap: 50e9 }) === false, "'mega' filter rejects $50B stock");
  assert(matchesSources('ETF', { marketCap: 0 }) === false, "'mega' filter rejects symbol with 0 marketCap (ETF)");

  // --- Test Set-based filters ---
  state.activeSourceMode = new Set(['topAUM']);
  assert(matchesSources('SPY', {}) === true, "'topAUM' filter matches SPY (in set)");
  assert(matchesSources('AAPL', {}) === false, "'topAUM' filter rejects AAPL (not in set)");

  state.activeSourceMode = new Set(['spdr']);
  assert(matchesSources('XLE', {}) === true, "'spdr' filter matches XLE (in set)");
  assert(matchesSources('XLK', {}) === true, "'spdr' filter matches XLK (in set)");

  // --- Test Multi-select filters ---
  state.activeSourceMode = new Set(['small', 'topAUM']);
  assert(matchesSources('MID', { marketCap: 50e9 }) === true, "Multi-select matches stock in 'small'");
  assert(matchesSources('SPY', { marketCap: 0 }) === true, "Multi-select matches ETF in 'topAUM'");
  assert(matchesSources('TSLA', { marketCap: 600e9 }) === false, "Multi-select rejects Mega Cap not in 'topAUM'");

  // Print results
  const failed = assertions.filter(a => a.status === 'FAIL');
  assertions.forEach(a => console.log(`[${a.status}] ${a.message}`));

  if (failed.length > 0) {
    console.log(`\nFAIL: ${failed.length} tests failed.`);
    process.exit(1);
  } else {
    console.log(`\nSUCCESS: All ${assertions.length} tests passed.`);
  }
}

runTests();
