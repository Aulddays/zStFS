<?php
/**
 * realtime.php — Intraday realtime curve endpoint.
 *
 * Reads 0.5-min bars written by run_open.py from memcache,
 * aggregates them into 2.5-min resolution (5 sub-bars → 1 bar).
 *
 * Aggregation rules:
 *   - price: last non-empty sub-bar price
 *   - outer/inner: sum of 5 sub-bars, separately
 *
 * Response format:
 *   {
 *     "date": "20261118",
 *     "bars": 65,
 *     "max_bars": 96,
 *     "data": {
 *       "sh510300": { "price": [...], "outer": [...], "inner": [...] },
 *       ...
 *     }
 *   }
 */

header('Content-Type: application/json; charset=utf-8');
header('Cache-Control: no-cache, no-store, must-revalidate');

// ---- config: read MEMCACHE_ADDR from ../.env ----
function _env_get($key, $default = null)
{
	$env_path = __DIR__ . '/../.env';
	if (!file_exists($env_path)) return $default;
	$lines = file($env_path, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES);
	foreach ($lines as $line)
	{
		$line = trim($line);
		if ($line === '' || strpos($line, '#') === 0) continue;
		$eq = strpos($line, '=');
		if ($eq === false) continue;
		$k = trim(substr($line, 0, $eq));
		$v = trim(substr($line, $eq + 1));
		$v = trim($v, "\"' \t");
		if ($k === $key) return $v;
	}
	return $default;
}
$MEMCACHE_ADDR = _env_get('MEMCACHE_ADDR', '127.0.0.1:11211');
$MEMCACHE_KEY  = 'zStFS.gjd.realtime';
$SUB_BARS = 10;      // 10 x 0.5min → 1 x 5min
$TOTAL_BARS = 48;    // 480 / 10

// ---- memcache read ----
$m = new Memcached();
$parts = explode(':', $MEMCACHE_ADDR);
$host = $parts[0];
$port = isset($parts[1]) ? (int)$parts[1] : 11211;
$m->addServer($host, $port);

$raw = $m->get($MEMCACHE_KEY);
$m->quit();

if ($raw === false)
{
	echo json_encode(['date' => null, 'bars' => 0, 'max_bars' => $TOTAL_BARS, 'data' => new stdClass()]);
	exit;
}

$store = json_decode($raw, true);
if (!is_array($store))
{
	echo json_encode(['date' => null, 'bars' => 0, 'max_bars' => $TOTAL_BARS, 'data' => new stdClass()]);
	exit;
}

// pick the latest date key
$dates = array_keys($store);
rsort($dates);
$date = $dates[0];
$day_data = $store[$date];

// ---- aggregate 0.5min → 2.5min ----
$result = [];
foreach ($day_data as $tcode => $cells)
{
	$n = count($cells);
	$price = [];
	$outer = [];
	$inner = [];

	for ($i = 0; $i < $TOTAL_BARS; $i++)
	{
		$start = $i * $SUB_BARS;
		$end   = $start + $SUB_BARS;
		if ($start >= $n)
			break;

		// price: last non-null, non-zero sub-bar
		$last_price = null;
		for ($j = min($end, $n) - 1; $j >= $start; $j--)
		{
			if (isset($cells[$j][0]) && $cells[$j][0] !== null && $cells[$j][0] !== 0)
			{
				$last_price = $cells[$j][0];
				break;
			}
		}

		// volume: sum across sub-bars
		$sum_outer = 0;
		$sum_inner = 0;
		for ($j = $start; $j < $end && $j < $n; $j++)
		{
			if (isset($cells[$j][1]) && is_numeric($cells[$j][1]))
				$sum_outer += $cells[$j][1];
			if (isset($cells[$j][2]) && is_numeric($cells[$j][2]))
				$sum_inner += $cells[$j][2];
		}

		$price[] = $last_price;
		$outer[] = $sum_outer;
		$inner[] = $sum_inner;
	}

	$result[$tcode] = [
		'price' => $price,
		'outer' => $outer,
		'inner' => $inner,
	];
}

// actual bar count = longest series across all ETFs
$actual_bars = 0;
foreach ($result as $r)
	$actual_bars = max($actual_bars, count($r['price']));

echo json_encode([
	'date' => $date,
	'bars' => $actual_bars,
	'max_bars' => $TOTAL_BARS,
	'data' => $result,
]);
