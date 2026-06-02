# 5v5 Real-Time Competitive Matchmaker

A runnable Rust service that keeps waiting players in memory, groups compatible
players into 5v5 matches, balances the teams, and exposes low-cost health and
metrics endpoints. The repository also includes a Python load simulator that can
inject thousands of concurrent player enqueue requests.

## Deliverables

- Working code: `src/main.rs`
- Simulation script: `scripts/simulate_load.py`
- ReadMe document: `README.md`

## Quick start

```bash
cargo test
cargo run --release -- --addr 127.0.0.1:8080 --workers 8
```

In another terminal:

```bash
python3 scripts/simulate_load.py --players 10000 --concurrency 256 --no-start-server
```

The simulator can also start the service automatically:

```bash
python3 scripts/simulate_load.py --players 10000 --concurrency 256
```

## Simulation result

I ran the load simulator with 10,000 players and 256 concurrent request workers:

```bash
python3 scripts/simulate_load.py --players 10000 --concurrency 256 --no-start-server
```

Result:

```text
Request status counts: {202: 10000}
Injection time: 1.488s
Throughput: 6722.0 enqueue req/s
Enqueue latency ms: p50=19.26 p95=50.44 p99=65.29
```

Final metrics:

```json
{
  "avg_enqueue_ns": 778,
  "avg_match_skill_spread": 200,
  "avg_team_skill_diff": 14,
  "avg_wait_ms": 387,
  "enqueue_rejections": 0,
  "matches_created": 997,
  "players_enqueued": 10000,
  "players_matched": 9970,
  "uptime_ms": 79706,
  "waiting_players": 30
}
```

The 30 remaining waiting players are expected in this run because players are
partitioned by region and mode, and each completed match requires 10 compatible
players in the same queue.

## API

### Enqueue player

```bash
curl -X POST http://127.0.0.1:8080/enqueue \
  -H 'Content-Type: application/json' \
  -d '{"player_id":"p1","skill":1830,"region":"na","mode":"ranked"}'
```

Required fields:

- `player_id`: unique player identifier
- `skill`: integer MMR from `0` to `5000`
- `region`: matchmaking region
- `mode`: queue or game mode

### Check player

```bash
curl http://127.0.0.1:8080/player/p1
```

### Metrics

```bash
curl http://127.0.0.1:8080/metrics
```

Example response:

```json
{
  "players_enqueued": 10000,
  "players_matched": 9950,
  "waiting_players": 50,
  "matches_created": 995,
  "enqueue_rejections": 0,
  "avg_wait_ms": 245,
  "avg_team_skill_diff": 31,
  "avg_match_skill_spread": 205,
  "avg_enqueue_ns": 9321,
  "uptime_ms": 4100
}
```

### Recent matches

```bash
curl 'http://127.0.0.1:8080/matches?limit=5'
```

## Algorithm

The service divides the rating range into fixed skill buckets of 100 MMR. Each
bucket is protected by its own mutex. Matching workers continuously scan buckets,
choose the oldest player in a bucket as the anchor, then inspect a bounded skill
window around that anchor.

Compatibility requires:

- same `region`
- same `mode`
- skill difference within the current relaxed range

The base range is 150 MMR. Every 2 seconds of wait time increases the range by
250 MMR, capped at the full 5000 MMR skill range. This handles the latency
versus quality trade-off: new players get tight matches, while outliers
eventually get a wide enough search window to avoid waiting indefinitely.

When at least 10 compatible players are available, the worker selects the ten
closest candidates, preferring older players as a tie-breaker. It then atomically
evicts those players by retaining locks on every bucket in the candidate window
while removing the selected ids. This prevents two workers from placing the same
player into different matches.

## Team balance

After finding 10 compatible players, the service evaluates every 5-player split
of those 10 players. There are only `C(10, 5) = 252` combinations, so exhaustive
search is cheap and gives the best team skill balance for the selected lobby.
The implementation fixes one player to team A to avoid evaluating mirrored
duplicates.

## Thread safety

- Waiting players live in per-skill-bucket `Mutex<VecDeque<Player>>` queues.
- Workers lock bucket windows in ascending bucket order, so there is no lock
  cycle between matching workers.
- Selected players are removed while the relevant bucket locks are held.
- Player ids and match assignments use separate mutex-protected maps.
- Metrics use atomics, so `/metrics` does not need to scan or lock the player
  pool.

The enqueue path takes the id lock only long enough to enforce uniqueness, then
takes exactly one bucket lock to append the player. The matching path avoids a
global pool lock, so workers can operate on disjoint rating windows at the same
time.

## Complexity

Let `B` be the number of buckets in a relaxed search window and `P` be the
number of players inside those buckets.

- Enqueue: `O(1)` expected time and `O(1)` extra space.
- Candidate scan: `O(P)`.
- Candidate ordering: `O(P log P)` in the current implementation.
- Atomic eviction: `O(P)` across the locked bucket window.
- Team split optimization: `O(C(10, 5))`, effectively constant.
- Metrics snapshot: `O(1)` because counters are atomic.

Space complexity is `O(N)` for `N` waiting players plus recent match history and
assignment maps.

## Scaling discussion

This implementation is intentionally in-memory and single-process, which keeps
latency low and makes atomic eviction straightforward. To scale further:

- Shard by `region`, `mode`, and broad skill band so most workers never contend.
- Run independent matchmaker processes per shard behind an ingress service.
- Use consistent hashing so reconnects and duplicate prevention route to the
  same shard.
- Keep the hot path in memory, but publish created matches and aggregate metrics
  asynchronously to Kafka, Redis Streams, or another durable system.
- Add backpressure per shard when queue depth or enqueue latency crosses a
  threshold.
- Use a richer quality function that includes role, party size, platform, ping,
  recent rematches, and uncertainty around skill rating.

## Notes

The HTTP server is implemented with the Rust standard library so the project can
build without downloading crates. It is enough to demonstrate the matchmaking
engine, concurrency model, and load behavior. For production HTTP handling, the
same `Matchmaker` core can sit behind `tokio` and `axum`.
