<?php
/**
 * api.php — GJD frontend API router.
 *
 * Endpoints:
 *   /api/realtime         Intraday realtime curve (5min bars, all ETFs)
 *   /api/daily/<code>     Daily data for one ETF (last N days, from zstore)
 *
 * zstore gjd-day row fields:
 *   date (int YYYYMMDD), code, open, high, low, close, volume (lots),
 *   turnover (yi), shares (yi), outer (lots), inner (lots)
 */

header('Content-Type: application/json; charset=utf-8');
header('Cache-Control: no-cache, no-store, must-revalidate');

// ---- config: read .env ----
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

// ---- parse request path ----
// REQUEST_URI like /gjd/fe/api/realtime?r=xxx or /gjd/fe/api/daily/sh510300
$uri = $_SERVER['REQUEST_URI'] ?? '';
// strip query string
$qpos = strpos($uri, '?');
if ($qpos !== false) $uri = substr($uri, 0, $qpos);

// extract path after "api/"
$api_pos = strpos($uri, 'api/');
if ($api_pos === false)
{
	http_response_code(404);
	echo json_encode(['error' => 'not found']);
	exit;
}
$path = substr($uri, $api_pos + 4);
$path = trim($path, '/');
$parts = explode('/', $path);

// ---- routes ----
if ($parts[0] === 'realtime')
{
	handle_realtime();
}
else if ($parts[0] === 'daily' && isset($parts[1]))
{
	handle_daily($parts[1]);
}
else
{
	http_response_code(404);
	echo json_encode(['error' => 'not found']);
}

// =================== realtime ===================
// Reads 0.5-min bars written by run_open.py from memcache,
// aggregates them into 5-min resolution (10 sub-bars → 1 bar).
//   {
//     "date": "20261118",
//     "bars": 48,
//     "max_bars": 48,
//     "data": {
//       "sh510300": { "price": [...], "outer": [...], "inner": [...] },
//       ...
//     }
//   }
function handle_realtime()
{
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

	// ---- aggregate 0.5min -> 5min ----
	$result = [];
	foreach ($day_data as $code => $cells)
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

		$result[$code] = [
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
}

// =================== daily ===================
function handle_daily($code)
{
	// validate code format: sh/sz + 6 digits
	if (!preg_match('/^(sh|sz)\d{6}$/', $code))
	{
		http_response_code(400);
		echo json_encode(['error' => 'invalid code']);
		return;
	}

	$zstore_url = _env_get('ZSTORE_URL');
	$zstore_auth = _env_get('ZSTORE_AUTHKEY');
	$zstore_table = _env_get('ZSTORE_TABLE', 'zStFS');

	if (!$zstore_url || !$zstore_auth)
	{
		http_response_code(503);
		echo json_encode(['error' => 'zstore not configured']);
		return;
	}

	require_once __DIR__ . '/../zstorec/zstorec.php';

	try
	{
		$z = new zstore($zstore_url, 'cosmos', $zstore_auth);
	}
	catch (Exception $e)
	{
		http_response_code(500);
		echo json_encode(['error' => "zstore init failed"]);
		return;
	}

	// query last 65 days (enough for 60 trading days)
	$limit = 65;

	try
	{
		$rows = $z->select(
			$zstore_table,
			['date', 'code', 'open', 'high', 'low', 'close', 'volume', 'turnover', 'shares', 'outer', 'inner'],
			['and' => [
				['partkey_', '=', 'gjd-day'],
				['code', '=', $code],
			]],
			null,
			[['date', 'DESC']],
			$limit
		);
	}
	catch (Exception $e)
	{
		http_response_code(500);
		echo json_encode(['error' => "zstore query failed"]);
		return;
	}

	// sort ascending by date
	$rows = is_array($rows) ? $rows : [];
	usort($rows, function($a, $b) {
		return $a['date'] - $b['date'];
	});

	// normalize: date as string, field names consistent
	$data = [];
	foreach ($rows as $r)
	{
		$d = isset($r['date']) ? (string)$r['date'] : '';
		if (strlen($d) === 8)
			$data[] = [
				'date'       => $d,
				'open'       => isset($r['open'])     ? (float)$r['open']     : null,
				'high'       => isset($r['high'])     ? (float)$r['high']     : null,
				'low'        => isset($r['low'])      ? (float)$r['low']      : null,
				'close'      => isset($r['close'])    ? (float)$r['close']    : null,
				'volume_lots'=> isset($r['volume'])   ? (float)$r['volume']   : 0,
				'amount_yi'  => isset($r['turnover']) ? (float)$r['turnover'] : 0,
				'shares_yi'  => isset($r['shares'])   ? (float)$r['shares']   : null,
				'outer_lots' => isset($r['outer'])    ? (float)$r['outer']    : null,
				'inner_lots' => isset($r['inner'])    ? (float)$r['inner']    : null,
			];
	}

	echo json_encode([
		'code' => $code,
		'days' => count($data),
		'data' => $data,
	]);
}
