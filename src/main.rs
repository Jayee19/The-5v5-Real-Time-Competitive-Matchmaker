use std::collections::{HashMap, HashSet, VecDeque};
use std::env;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const TEAM_SIZE: usize = 5;
const MATCH_SIZE: usize = TEAM_SIZE * 2;
const BUCKET_SIZE: u32 = 100;
const MAX_SKILL: u32 = 5000;
const BUCKET_COUNT: usize = (MAX_SKILL as usize / BUCKET_SIZE as usize) + 1;
const RECENT_MATCH_LIMIT: usize = 100;
const BASE_RANGE: u32 = 150;
const RELAX_STEP_RANGE: u32 = 250;
const RELAX_STEP_SECS: u64 = 2;
const MAX_RANGE: u32 = MAX_SKILL;

#[derive(Clone, Debug)]
struct Player {
    id: String,
    skill: u32,
    region: String,
    mode: String,
    enqueued_at: Instant,
    seq: u64,
}

#[derive(Clone, Debug)]
struct TeamMember {
    id: String,
    skill: u32,
}

#[derive(Clone, Debug)]
struct MatchRecord {
    id: u64,
    team_a: Vec<TeamMember>,
    team_b: Vec<TeamMember>,
    skill_diff: u32,
    skill_spread: u32,
    average_wait_ms: u64,
    created_epoch_ms: u128,
}

#[derive(Clone, Debug)]
struct PlayerAssignment {
    match_id: u64,
    team: &'static str,
}

#[derive(Debug)]
struct PlayerRequest {
    player_id: String,
    skill: u32,
    region: String,
    mode: String,
}

#[derive(Debug)]
enum EnqueueError {
    Duplicate,
    Invalid(String),
}

struct Matchmaker {
    buckets: Vec<Mutex<VecDeque<Player>>>,
    waiting_ids: Mutex<HashSet<String>>,
    assignments: Mutex<HashMap<String, PlayerAssignment>>,
    recent_matches: Mutex<VecDeque<MatchRecord>>,
    next_player_seq: AtomicU64,
    next_match_id: AtomicU64,
    players_enqueued: AtomicU64,
    players_matched: AtomicU64,
    matches_created: AtomicU64,
    enqueue_rejections: AtomicU64,
    total_wait_ms: AtomicU64,
    total_skill_diff: AtomicU64,
    total_skill_spread: AtomicU64,
    total_enqueue_ns: AtomicU64,
    start_time: Instant,
}

impl Matchmaker {
    fn new() -> Self {
        let mut buckets = Vec::with_capacity(BUCKET_COUNT);
        for _ in 0..BUCKET_COUNT {
            buckets.push(Mutex::new(VecDeque::new()));
        }

        Self {
            buckets,
            waiting_ids: Mutex::new(HashSet::new()),
            assignments: Mutex::new(HashMap::new()),
            recent_matches: Mutex::new(VecDeque::new()),
            next_player_seq: AtomicU64::new(1),
            next_match_id: AtomicU64::new(1),
            players_enqueued: AtomicU64::new(0),
            players_matched: AtomicU64::new(0),
            matches_created: AtomicU64::new(0),
            enqueue_rejections: AtomicU64::new(0),
            total_wait_ms: AtomicU64::new(0),
            total_skill_diff: AtomicU64::new(0),
            total_skill_spread: AtomicU64::new(0),
            total_enqueue_ns: AtomicU64::new(0),
            start_time: Instant::now(),
        }
    }

    fn enqueue(&self, request: PlayerRequest) -> Result<(), EnqueueError> {
        let start = Instant::now();
        validate_request(&request)?;

        {
            let mut waiting_ids = lock(&self.waiting_ids);
            if waiting_ids.contains(&request.player_id)
                || lock(&self.assignments).contains_key(&request.player_id)
            {
                self.enqueue_rejections.fetch_add(1, Ordering::Relaxed);
                return Err(EnqueueError::Duplicate);
            }
            waiting_ids.insert(request.player_id.clone());
        }

        let player = Player {
            id: request.player_id,
            skill: request.skill,
            region: request.region,
            mode: request.mode,
            enqueued_at: Instant::now(),
            seq: self.next_player_seq.fetch_add(1, Ordering::Relaxed),
        };

        let bucket_idx = skill_bucket(player.skill);
        lock(&self.buckets[bucket_idx]).push_back(player);

        self.players_enqueued.fetch_add(1, Ordering::Relaxed);
        self.total_enqueue_ns.fetch_add(
            start.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
        Ok(())
    }

    fn start_workers(self: &Arc<Self>, worker_count: usize) {
        for worker_id in 0..worker_count {
            let engine = Arc::clone(self);
            thread::spawn(move || matching_worker(worker_id, engine));
        }
    }

    fn try_form_match_from_bucket(&self, bucket_idx: usize) -> Option<MatchRecord> {
        let anchor = {
            let bucket = lock(&self.buckets[bucket_idx]);
            bucket.front().cloned()
        }?;

        let allowed_range = relaxation_range(anchor.enqueued_at.elapsed());
        let low_bucket = skill_bucket(anchor.skill.saturating_sub(allowed_range));
        let high_bucket = skill_bucket(anchor.skill.saturating_add(allowed_range).min(MAX_SKILL));

        let mut guards: Vec<(usize, MutexGuard<'_, VecDeque<Player>>)> =
            Vec::with_capacity(high_bucket - low_bucket + 1);
        for idx in low_bucket..=high_bucket {
            guards.push((idx, lock(&self.buckets[idx])));
        }

        let anchor_still_waiting = guards
            .iter()
            .find(|(idx, _)| *idx == bucket_idx)
            .and_then(|(_, bucket)| bucket.iter().find(|player| player.id == anchor.id))
            .is_some();

        if !anchor_still_waiting {
            return None;
        }

        let mut candidates = Vec::with_capacity(MATCH_SIZE * 3);
        for (_, bucket) in &guards {
            for player in bucket.iter() {
                if is_compatible(&anchor, player) {
                    candidates.push(player.clone());
                }
            }
        }

        if candidates.len() < MATCH_SIZE {
            return None;
        }

        candidates.sort_by_key(|player| candidate_rank(&anchor, player));
        let selected: Vec<Player> = candidates.into_iter().take(MATCH_SIZE).collect();
        if !selected.iter().any(|player| player.id == anchor.id) {
            return None;
        }

        let selected_ids: HashSet<&str> =
            selected.iter().map(|player| player.id.as_str()).collect();
        let mut removed = 0usize;
        for (_, bucket) in guards.iter_mut() {
            let before = bucket.len();
            bucket.retain(|player| !selected_ids.contains(player.id.as_str()));
            removed += before - bucket.len();
        }

        drop(guards);

        if removed != MATCH_SIZE {
            return None;
        }

        Some(self.record_match(selected))
    }

    fn record_match(&self, players: Vec<Player>) -> MatchRecord {
        let match_id = self.next_match_id.fetch_add(1, Ordering::Relaxed);
        let (team_a_players, team_b_players, skill_diff) = balance_teams(&players);
        let now = Instant::now();
        let wait_sum_ms: u64 = players
            .iter()
            .map(|player| {
                now.duration_since(player.enqueued_at)
                    .as_millis()
                    .min(u64::MAX as u128) as u64
            })
            .sum();
        let average_wait_ms = wait_sum_ms / MATCH_SIZE as u64;
        let min_skill = players.iter().map(|player| player.skill).min().unwrap_or(0);
        let max_skill = players.iter().map(|player| player.skill).max().unwrap_or(0);
        let skill_spread = max_skill.saturating_sub(min_skill);

        let team_a = to_team_members(&team_a_players);
        let team_b = to_team_members(&team_b_players);
        let record = MatchRecord {
            id: match_id,
            team_a,
            team_b,
            skill_diff,
            skill_spread,
            average_wait_ms,
            created_epoch_ms: epoch_ms(),
        };

        {
            let mut waiting_ids = lock(&self.waiting_ids);
            for player in &players {
                waiting_ids.remove(&player.id);
            }
        }

        {
            let mut assignments = lock(&self.assignments);
            for player in &team_a_players {
                assignments.insert(
                    player.id.clone(),
                    PlayerAssignment {
                        match_id,
                        team: "A",
                    },
                );
            }
            for player in &team_b_players {
                assignments.insert(
                    player.id.clone(),
                    PlayerAssignment {
                        match_id,
                        team: "B",
                    },
                );
            }
        }

        {
            let mut recent_matches = lock(&self.recent_matches);
            recent_matches.push_front(record.clone());
            while recent_matches.len() > RECENT_MATCH_LIMIT {
                recent_matches.pop_back();
            }
        }

        self.matches_created.fetch_add(1, Ordering::Relaxed);
        self.players_matched
            .fetch_add(MATCH_SIZE as u64, Ordering::Relaxed);
        self.total_wait_ms.fetch_add(wait_sum_ms, Ordering::Relaxed);
        self.total_skill_diff
            .fetch_add(skill_diff as u64, Ordering::Relaxed);
        self.total_skill_spread
            .fetch_add(skill_spread as u64, Ordering::Relaxed);

        record
    }

    fn player_state_json(&self, player_id: &str) -> Option<String> {
        if let Some(assignment) = lock(&self.assignments).get(player_id).cloned() {
            return Some(format!(
                "{{\"state\":\"matched\",\"match_id\":{},\"team\":\"{}\"}}",
                assignment.match_id, assignment.team
            ));
        }

        if lock(&self.waiting_ids).contains(player_id) {
            return Some("{\"state\":\"queued\"}".to_string());
        }

        None
    }

    fn metrics_json(&self) -> String {
        let enqueued = self.players_enqueued.load(Ordering::Relaxed);
        let matched = self.players_matched.load(Ordering::Relaxed);
        let matches = self.matches_created.load(Ordering::Relaxed);
        let rejected = self.enqueue_rejections.load(Ordering::Relaxed);
        let total_wait_ms = self.total_wait_ms.load(Ordering::Relaxed);
        let total_skill_diff = self.total_skill_diff.load(Ordering::Relaxed);
        let total_skill_spread = self.total_skill_spread.load(Ordering::Relaxed);
        let total_enqueue_ns = self.total_enqueue_ns.load(Ordering::Relaxed);
        let waiting = enqueued.saturating_sub(matched);
        let avg_wait_ms = total_wait_ms.checked_div(matched).unwrap_or(0);
        let avg_skill_diff = total_skill_diff.checked_div(matches).unwrap_or(0);
        let avg_skill_spread = total_skill_spread.checked_div(matches).unwrap_or(0);
        let avg_enqueue_ns = total_enqueue_ns.checked_div(enqueued).unwrap_or(0);
        let uptime_ms = self.start_time.elapsed().as_millis();

        format!(
            concat!(
                "{{",
                "\"players_enqueued\":{},",
                "\"players_matched\":{},",
                "\"waiting_players\":{},",
                "\"matches_created\":{},",
                "\"enqueue_rejections\":{},",
                "\"avg_wait_ms\":{},",
                "\"avg_team_skill_diff\":{},",
                "\"avg_match_skill_spread\":{},",
                "\"avg_enqueue_ns\":{},",
                "\"uptime_ms\":{}",
                "}}"
            ),
            enqueued,
            matched,
            waiting,
            matches,
            rejected,
            avg_wait_ms,
            avg_skill_diff,
            avg_skill_spread,
            avg_enqueue_ns,
            uptime_ms
        )
    }

    fn recent_matches_json(&self, limit: usize) -> String {
        let recent_matches = lock(&self.recent_matches);
        let mut body = String::from("{\"matches\":[");
        for (idx, record) in recent_matches.iter().take(limit).enumerate() {
            if idx > 0 {
                body.push(',');
            }
            body.push_str(&match_record_json(record));
        }
        body.push_str("]}");
        body
    }
}

fn matching_worker(worker_id: usize, engine: Arc<Matchmaker>) {
    let mut cursor = worker_id % BUCKET_COUNT;
    loop {
        let mut made_match = false;
        for offset in 0..BUCKET_COUNT {
            let bucket_idx = (cursor + offset) % BUCKET_COUNT;
            if engine.try_form_match_from_bucket(bucket_idx).is_some() {
                made_match = true;
            }
        }
        cursor = (cursor + 1) % BUCKET_COUNT;
        if !made_match {
            thread::sleep(Duration::from_millis(5));
        }
    }
}

fn validate_request(request: &PlayerRequest) -> Result<(), EnqueueError> {
    if request.player_id.trim().is_empty() {
        return Err(EnqueueError::Invalid(
            "player_id must not be empty".to_string(),
        ));
    }
    if request.skill > MAX_SKILL {
        return Err(EnqueueError::Invalid(format!(
            "skill must be <= {}",
            MAX_SKILL
        )));
    }
    if request.region.trim().is_empty() {
        return Err(EnqueueError::Invalid(
            "region must not be empty".to_string(),
        ));
    }
    if request.mode.trim().is_empty() {
        return Err(EnqueueError::Invalid("mode must not be empty".to_string()));
    }
    Ok(())
}

fn is_compatible(anchor: &Player, candidate: &Player) -> bool {
    if anchor.region != candidate.region || anchor.mode != candidate.mode {
        return false;
    }
    let anchor_range = relaxation_range(anchor.enqueued_at.elapsed());
    let candidate_range = relaxation_range(candidate.enqueued_at.elapsed());
    let shared_range = anchor_range.max(candidate_range);
    anchor.skill.abs_diff(candidate.skill) <= shared_range
}

fn relaxation_range(wait: Duration) -> u32 {
    let steps = wait.as_secs() / RELAX_STEP_SECS;
    BASE_RANGE
        .saturating_add((steps as u32).saturating_mul(RELAX_STEP_RANGE))
        .min(MAX_RANGE)
}

fn candidate_rank(anchor: &Player, candidate: &Player) -> (u32, u64, u64) {
    let wait_ms = candidate
        .enqueued_at
        .elapsed()
        .as_millis()
        .min(u64::MAX as u128) as u64;
    (
        anchor.skill.abs_diff(candidate.skill),
        u64::MAX - wait_ms,
        candidate.seq,
    )
}

fn balance_teams(players: &[Player]) -> (Vec<Player>, Vec<Player>, u32) {
    debug_assert_eq!(players.len(), MATCH_SIZE);
    let total_skill: u32 = players.iter().map(|player| player.skill).sum();
    let mut best_mask = 0u16;
    let mut best_diff = u32::MAX;

    for mask in 0u16..(1u16 << MATCH_SIZE) {
        if mask & 1 == 0 || mask.count_ones() as usize != TEAM_SIZE {
            continue;
        }

        let team_a_skill: u32 = players
            .iter()
            .enumerate()
            .filter(|(idx, _)| mask & (1u16 << idx) != 0)
            .map(|(_, player)| player.skill)
            .sum();
        let team_b_skill = total_skill - team_a_skill;
        let diff = team_a_skill.abs_diff(team_b_skill);

        if diff < best_diff {
            best_diff = diff;
            best_mask = mask;
        }
    }

    let mut team_a = Vec::with_capacity(TEAM_SIZE);
    let mut team_b = Vec::with_capacity(TEAM_SIZE);
    for (idx, player) in players.iter().enumerate() {
        if best_mask & (1u16 << idx) != 0 {
            team_a.push(player.clone());
        } else {
            team_b.push(player.clone());
        }
    }

    (team_a, team_b, best_diff)
}

fn to_team_members(players: &[Player]) -> Vec<TeamMember> {
    players
        .iter()
        .map(|player| TeamMember {
            id: player.id.clone(),
            skill: player.skill,
        })
        .collect()
}

fn skill_bucket(skill: u32) -> usize {
    (skill.min(MAX_SKILL) / BUCKET_SIZE) as usize
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn main() -> std::io::Result<()> {
    let (addr, workers) = parse_args();
    let listener = TcpListener::bind(&addr)?;
    let engine = Arc::new(Matchmaker::new());
    engine.start_workers(workers);

    println!(
        "matchmaker listening on http://{} with {} workers",
        addr, workers
    );
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let engine = Arc::clone(&engine);
                thread::spawn(move || {
                    if let Err(err) = handle_connection(stream, engine) {
                        eprintln!("connection error: {}", err);
                    }
                });
            }
            Err(err) => eprintln!("accept error: {}", err),
        }
    }

    Ok(())
}

fn parse_args() -> (String, usize) {
    let mut addr = "127.0.0.1:8080".to_string();
    let mut workers = thread::available_parallelism()
        .map(|value| value.get())
        .unwrap_or(4)
        .max(2);

    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--addr" => {
                if let Some(value) = args.next() {
                    addr = value;
                }
            }
            "--workers" => {
                if let Some(value) = args.next() {
                    workers = value.parse::<usize>().unwrap_or(workers).max(1);
                }
            }
            _ => {}
        }
    }

    (addr, workers)
}

fn handle_connection(mut stream: TcpStream, engine: Arc<Matchmaker>) -> std::io::Result<()> {
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    let request = match read_http_request(&mut stream)? {
        Some(request) => request,
        None => return Ok(()),
    };

    let response = route_request(&request, &engine);
    stream.write_all(response.as_bytes())?;
    stream.flush()?;
    Ok(())
}

struct HttpRequest {
    method: String,
    path: String,
    body: String,
}

fn read_http_request(stream: &mut TcpStream) -> std::io::Result<Option<HttpRequest>> {
    let mut data = Vec::with_capacity(4096);
    let mut buffer = [0u8; 2048];
    let mut headers_end = None;
    let mut expected_len = None;

    loop {
        let read = stream.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        data.extend_from_slice(&buffer[..read]);

        if headers_end.is_none() {
            headers_end = find_subsequence(&data, b"\r\n\r\n").map(|idx| idx + 4);
            if let Some(end) = headers_end {
                let headers = String::from_utf8_lossy(&data[..end]);
                let content_length = parse_content_length(&headers).unwrap_or(0);
                expected_len = Some(end + content_length);
            }
        }

        if let Some(len) = expected_len
            && data.len() >= len
        {
            break;
        }

        if data.len() > 64 * 1024 {
            break;
        }
    }

    let headers_end = match headers_end {
        Some(value) => value,
        None => return Ok(None),
    };
    let header_text = String::from_utf8_lossy(&data[..headers_end]);
    let mut lines = header_text.lines();
    let first_line = match lines.next() {
        Some(value) => value,
        None => return Ok(None),
    };
    let mut parts = first_line.split_whitespace();
    let method = parts.next().unwrap_or("").to_string();
    let path = parts.next().unwrap_or("/").to_string();
    let body_len = parse_content_length(&header_text).unwrap_or(0);
    let body_end = headers_end + body_len;
    let body = String::from_utf8_lossy(&data[headers_end..body_end.min(data.len())]).to_string();

    Ok(Some(HttpRequest { method, path, body }))
}

fn route_request(request: &HttpRequest, engine: &Matchmaker) -> String {
    let path_without_query = request.path.split('?').next().unwrap_or("/");

    match (request.method.as_str(), path_without_query) {
        ("GET", "/health") => http_response(200, "{\"status\":\"ok\"}"),
        ("GET", "/metrics") => http_response(200, &engine.metrics_json()),
        ("GET", "/matches") => {
            let limit = query_param(&request.path, "limit")
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(20)
                .min(RECENT_MATCH_LIMIT);
            http_response(200, &engine.recent_matches_json(limit))
        }
        ("POST", "/enqueue") => match parse_player_request(&request.body) {
            Some(player_request) => match engine.enqueue(player_request) {
                Ok(()) => http_response(202, "{\"status\":\"queued\"}"),
                Err(EnqueueError::Duplicate) => {
                    http_response(409, "{\"error\":\"player is already queued or matched\"}")
                }
                Err(EnqueueError::Invalid(message)) => {
                    http_response(400, &format!("{{\"error\":\"{}\"}}", json_escape(&message)))
                }
            },
            None => http_response(400, "{\"error\":\"invalid player payload\"}"),
        },
        ("GET", path) if path.starts_with("/player/") => {
            let player_id = percent_decode(&path["/player/".len()..]);
            match engine.player_state_json(&player_id) {
                Some(body) => http_response(200, &body),
                None => http_response(404, "{\"error\":\"player not found\"}"),
            }
        }
        _ => http_response(404, "{\"error\":\"not found\"}"),
    }
}

fn http_response(status: u16, body: &str) -> String {
    let reason = match status {
        200 => "OK",
        202 => "Accepted",
        400 => "Bad Request",
        404 => "Not Found",
        409 => "Conflict",
        _ => "Internal Server Error",
    };

    format!(
        "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        status,
        reason,
        body.len(),
        body
    )
}

fn parse_player_request(body: &str) -> Option<PlayerRequest> {
    let player_id = json_string(body, "player_id").or_else(|| json_string(body, "id"))?;
    let skill = json_u32(body, "skill")?;
    let region = json_string(body, "region").unwrap_or_else(|| "global".to_string());
    let mode = json_string(body, "mode").unwrap_or_else(|| "ranked".to_string());
    Some(PlayerRequest {
        player_id,
        skill,
        region,
        mode,
    })
}

fn json_string(body: &str, key: &str) -> Option<String> {
    let mut idx = body.find(&format!("\"{}\"", key))?;
    idx = body[idx..].find(':')? + idx + 1;
    let bytes = body.as_bytes();
    while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
        idx += 1;
    }
    if idx >= bytes.len() || bytes[idx] != b'"' {
        return None;
    }
    idx += 1;
    let mut output = String::new();
    let mut escaped = false;
    for ch in body[idx..].chars() {
        if escaped {
            output.push(match ch {
                '"' => '"',
                '\\' => '\\',
                'n' => '\n',
                'r' => '\r',
                't' => '\t',
                other => other,
            });
            escaped = false;
            continue;
        }
        match ch {
            '\\' => escaped = true,
            '"' => return Some(output),
            other => output.push(other),
        }
    }
    None
}

fn json_u32(body: &str, key: &str) -> Option<u32> {
    let mut idx = body.find(&format!("\"{}\"", key))?;
    idx = body[idx..].find(':')? + idx + 1;
    let bytes = body.as_bytes();
    while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
        idx += 1;
    }
    let start = idx;
    while idx < bytes.len() && bytes[idx].is_ascii_digit() {
        idx += 1;
    }
    if idx == start {
        return None;
    }
    body[start..idx].parse::<u32>().ok()
}

fn parse_content_length(headers: &str) -> Option<usize> {
    for line in headers.lines() {
        if let Some((name, value)) = line.split_once(':')
            && name.eq_ignore_ascii_case("content-length")
        {
            return value.trim().parse::<usize>().ok();
        }
    }
    None
}

fn query_param(path: &str, key: &str) -> Option<String> {
    let query = path.split_once('?')?.1;
    for pair in query.split('&') {
        let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
        if name == key {
            return Some(percent_decode(value));
        }
    }
    None
}

fn percent_decode(value: &str) -> String {
    let mut output = Vec::with_capacity(value.len());
    let bytes = value.as_bytes();
    let mut idx = 0;
    while idx < bytes.len() {
        match bytes[idx] {
            b'%' if idx + 2 < bytes.len() => {
                if let Ok(hex) = u8::from_str_radix(&value[idx + 1..idx + 3], 16) {
                    output.push(hex);
                    idx += 3;
                } else {
                    output.push(bytes[idx]);
                    idx += 1;
                }
            }
            b'+' => {
                output.push(b' ');
                idx += 1;
            }
            byte => {
                output.push(byte);
                idx += 1;
            }
        }
    }
    String::from_utf8_lossy(&output).to_string()
}

fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn match_record_json(record: &MatchRecord) -> String {
    format!(
        concat!(
            "{{",
            "\"match_id\":{},",
            "\"created_epoch_ms\":{},",
            "\"skill_diff\":{},",
            "\"skill_spread\":{},",
            "\"average_wait_ms\":{},",
            "\"team_a\":{},",
            "\"team_b\":{}",
            "}}"
        ),
        record.id,
        record.created_epoch_ms,
        record.skill_diff,
        record.skill_spread,
        record.average_wait_ms,
        team_json(&record.team_a),
        team_json(&record.team_b)
    )
}

fn team_json(team: &[TeamMember]) -> String {
    let mut body = String::from("[");
    for (idx, member) in team.iter().enumerate() {
        if idx > 0 {
            body.push(',');
        }
        body.push_str(&format!(
            "{{\"player_id\":\"{}\",\"skill\":{}}}",
            json_escape(&member.id),
            member.skill
        ));
    }
    body.push(']');
    body
}

fn json_escape(value: &str) -> String {
    let mut output = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '"' => output.push_str("\\\""),
            '\\' => output.push_str("\\\\"),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\t' => output.push_str("\\t"),
            other => output.push(other),
        }
    }
    output
}

fn epoch_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn player(id: usize, skill: u32) -> Player {
        Player {
            id: format!("p{}", id),
            skill,
            region: "na".to_string(),
            mode: "ranked".to_string(),
            enqueued_at: Instant::now(),
            seq: id as u64,
        }
    }

    #[test]
    fn balances_teams_by_total_skill() {
        let players = vec![
            player(1, 1000),
            player(2, 1010),
            player(3, 1020),
            player(4, 1030),
            player(5, 1040),
            player(6, 1050),
            player(7, 1060),
            player(8, 1070),
            player(9, 1080),
            player(10, 1090),
        ];

        let (team_a, team_b, diff) = balance_teams(&players);
        assert_eq!(team_a.len(), TEAM_SIZE);
        assert_eq!(team_b.len(), TEAM_SIZE);
        assert_eq!(diff, 10);
    }

    #[test]
    fn relaxation_range_increases_with_wait_time() {
        assert_eq!(relaxation_range(Duration::from_secs(0)), BASE_RANGE);
        assert_eq!(
            relaxation_range(Duration::from_secs(RELAX_STEP_SECS)),
            BASE_RANGE + RELAX_STEP_RANGE
        );
        assert_eq!(relaxation_range(Duration::from_secs(1000)), MAX_RANGE);
    }

    #[test]
    fn forms_one_match_and_evicts_players_atomically() {
        let engine = Matchmaker::new();
        for idx in 0..MATCH_SIZE {
            engine
                .enqueue(PlayerRequest {
                    player_id: format!("player-{}", idx),
                    skill: 1500 + idx as u32,
                    region: "na".to_string(),
                    mode: "ranked".to_string(),
                })
                .unwrap();
        }

        let record = engine
            .try_form_match_from_bucket(skill_bucket(1500))
            .expect("expected a match");
        assert_eq!(record.team_a.len(), TEAM_SIZE);
        assert_eq!(record.team_b.len(), TEAM_SIZE);
        assert_eq!(
            engine.players_matched.load(Ordering::Relaxed),
            MATCH_SIZE as u64
        );
        assert_eq!(
            engine.players_enqueued.load(Ordering::Relaxed),
            MATCH_SIZE as u64
        );
        assert_eq!(lock(&engine.waiting_ids).len(), 0);
    }

    #[test]
    fn parses_basic_enqueue_payload() {
        let request = parse_player_request(
            r#"{"player_id":"abc","skill":1732,"region":"eu","mode":"ranked"}"#,
        )
        .unwrap();
        assert_eq!(request.player_id, "abc");
        assert_eq!(request.skill, 1732);
        assert_eq!(request.region, "eu");
        assert_eq!(request.mode, "ranked");
    }
}
