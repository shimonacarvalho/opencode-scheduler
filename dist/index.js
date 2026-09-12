// src/index.ts
import { tool } from "@opencode-ai/plugin";
import { createWriteStream, existsSync, mkdirSync, readdirSync, readFileSync, rmSync, writeFileSync, unlinkSync } from "fs";
import { basename, dirname, join, resolve as resolvePath } from "path";
import { homedir, platform } from "os";
import { execFileSync, execSync, spawn } from "child_process";
import { fileURLToPath } from "url";
var OPENCODE_CONFIG = join(homedir(), ".config", "opencode");
var LEGACY_JOBS_DIR = join(OPENCODE_CONFIG, "jobs");
var LOGS_DIR = join(OPENCODE_CONFIG, "logs");
var SCHEDULER_DIR = join(OPENCODE_CONFIG, "scheduler");
var SCOPES_DIR = join(SCHEDULER_DIR, "scopes");
var SUPERVISOR_PATH = join(SCHEDULER_DIR, "supervisor.pl");
var SCHEDULER_CONFIG = join(OPENCODE_CONFIG, "opencode-scheduler.json");
var WORKTREES_DIR = join(SCHEDULER_DIR, "worktrees");
var WORKTREE_WRAPPER_PATH = join(SCHEDULER_DIR, "worktree-run.sh");
var DEFAULT_WORKTREE_KEEP_HOURS = 24;
var IS_MAC = platform() === "darwin";
var IS_LINUX = platform() === "linux";
var IS_WINDOWS = platform() === "win32";
var LAUNCH_AGENTS_DIR = join(homedir(), "Library", "LaunchAgents");
var LAUNCHD_PREFIX = "com.opencode.job";
var SYSTEMD_USER_DIR = join(homedir(), ".config", "systemd", "user");
var WINDOWS_TASK_ROOT = "\\OpenCode";
var WINDOWS_TASK_PREFIX = "opencode-job";
var CRON_MANAGED_PREFIX = "opencode-scheduler";
function ensureDir(dir) {
  if (!existsSync(dir)) {
    mkdirSync(dir, { recursive: true });
  }
}
function slugify(name) {
  return name.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "");
}
function normalizeWorkdirPath(input) {
  const trimmed = input.trim();
  if (!trimmed) return homedir();
  return resolvePath(trimmed);
}
function fnv1a64(input) {
  let hash = 0xcbf29ce484222325n;
  const prime = 0x100000001b3n;
  const data = Buffer.from(input, "utf8");
  for (const byte of data) {
    hash ^= BigInt(byte);
    hash = hash * prime & 0xffffffffffffffffn;
  }
  return hash;
}
function fnv1a64Hex(input) {
  return fnv1a64(input).toString(16).padStart(16, "0");
}
function deriveScopeId(workdir) {
  const normalized = normalizeWorkdirPath(workdir);
  const base = slugify(basename(normalized)) || "workspace";
  const suffix = fnv1a64Hex(normalized).slice(0, 12);
  return `${base}-${suffix}`;
}
function scopeDir(scopeId) {
  return join(SCOPES_DIR, scopeId);
}
function scopeJobsDir(scopeId) {
  return join(scopeDir(scopeId), "jobs");
}
function scopeLocksDir(scopeId) {
  return join(scopeDir(scopeId), "locks");
}
function scopeRunsDir(scopeId) {
  return join(scopeDir(scopeId), "runs");
}
function scopeLogsDir(scopeId) {
  return join(LOGS_DIR, "scheduler", scopeId);
}
function jobFilePath(scopeId, slug) {
  return join(scopeJobsDir(scopeId), `${slug}.json`);
}
function scopedLogPath(scopeId, slug) {
  return join(scopeLogsDir(scopeId), `${slug}.log`);
}
function currentScopeId() {
  return deriveScopeId(process.cwd());
}
var SUPERVISOR_SCRIPT = `#!/usr/bin/perl
use strict;
use warnings;
use JSON::PP;
use File::Basename qw(dirname);
use File::Path qw(make_path);
use POSIX qw(setsid strftime);
use Time::HiRes qw(time);

# opencode-scheduler supervisor v1

sub iso_now {
  my @t = localtime(time());
  return strftime("%Y-%m-%dT%H:%M:%S%z", @t);
}

sub read_json {
  my ($path) = @_;
  open my $fh, "<", $path or die "Failed to read $path: $!
";
  local $/;
  my $raw = <$fh>;
  close $fh;
  my $json = JSON::PP->new->utf8->relaxed;
  return $json->decode($raw);
}

sub write_json_atomic {
  my ($path, $data) = @_;
  my $tmp = "$path.tmp.$$";
  my $json = JSON::PP->new->utf8->canonical;
  open my $fh, ">", $tmp or die "Failed to write $tmp: $!
";
  print $fh $json->encode($data);
  close $fh or die "Failed to close $tmp: $!
";
  rename $tmp, $path or die "Failed to rename $tmp -> $path: $!
";
}

sub append_jsonl {
  my ($path, $data) = @_;
  my $json = JSON::PP->new->utf8->canonical;
  open my $fh, ">>", $path or die "Failed to append $path: $!
";
  print $fh $json->encode($data) . "
";
  close $fh;
}

sub pid_alive {
  my ($pid) = @_;
  return 0 if !$pid;
  return kill 0, $pid;
}

sub random_id {
  my $n = int(rand(1_000_000_000));
  return sprintf("%09d", $n);
}

my $job_path = shift @ARGV;
if (!$job_path) { die "usage: supervisor.pl <job.json>
"; }

my $job = read_json($job_path);
my $scope_id = $job->{scopeId} || "";
my $slug = $job->{slug} || "";
if (!$scope_id || !$slug) { die "job missing scopeId/slug
"; }

my $home = $ENV{HOME} || "";
if (!$home) { die "HOME is not set
"; }

my $config_root = "$home/.config/opencode";
my $scheduler_root = "$config_root/scheduler/scopes/$scope_id";
my $locks_dir = "$scheduler_root/locks";
my $runs_dir = "$scheduler_root/runs";
my $logs_dir = "$config_root/logs/scheduler/$scope_id";

make_path($locks_dir);
make_path($runs_dir);
make_path($logs_dir);

my $log_path = "$logs_dir/$slug.log";
open STDOUT, ">>", $log_path or die "Failed to open log $log_path: $!
";
open STDERR, ">&STDOUT" or die "Failed to dup stderr: $!
";
select STDOUT; $| = 1;
select STDERR; $| = 1;

my $lock_path = "$locks_dir/$slug.json";
if (-e $lock_path) {
  my $lock = eval { read_json($lock_path) };
  my $pid = ($lock && ref($lock) eq 'HASH') ? ($lock->{pid} || 0) : 0;
  if (pid_alive($pid)) {
    my $now = iso_now();
    print "
=== Scheduled run skipped (already running pid=$pid) $now ===
";
    exit 0;
  }
  unlink $lock_path;
}

my $run_id = time() . "-" . random_id();
my $started_at = iso_now();
my $t0 = time();

write_json_atomic($lock_path, { pid => $$, startedAt => $started_at, runId => $run_id });

# Update job metadata: running
$job->{lastRunAt} = $started_at;
$job->{lastRunSource} = "scheduled";
$job->{lastRunStatus} = "running";
delete $job->{lastRunExitCode};
delete $job->{lastRunError};
$job->{updatedAt} = $started_at;
write_json_atomic($job_path, $job);

# Force non-interactive scheduled runs
my $perm = { question => "deny" };
if ($ENV{OPENCODE_PERMISSION}) {
  my $existing = eval { JSON::PP->new->decode($ENV{OPENCODE_PERMISSION}) };
  if ($existing && ref($existing) eq 'HASH') {
    $perm = { %$existing, %$perm };
  }
}
$ENV{OPENCODE_PERMISSION} = JSON::PP->new->canonical->encode($perm);
$ENV{OPENCODE_SCHEDULER_RUN_ID} = $run_id;

print "
=== Scheduled run $started_at runId=$run_id ===
";

my $inv = $job->{invocation};
if (!$inv || ref($inv) ne 'HASH' || !$inv->{command} || ref($inv->{args}) ne 'ARRAY') {
  my $now = iso_now();
  print "
=== Supervisor error $now: job missing invocation.command/args ===
";
  $job->{lastRunStatus} = "failed";
  $job->{lastRunError} = "job missing invocation";
  $job->{updatedAt} = $now;
  write_json_atomic($job_path, $job);
  unlink $lock_path;
  exit 1;
}

my $command = $inv->{command};
my @args = @{ $inv->{args} };

my $workdir = $job->{workdir} || $home;

my $timeout = $job->{timeoutSeconds};
$timeout = undef if defined($timeout) && $timeout !~ /^\\d+$/;

my $timed_out = 0;
my $child_pid = fork();
if (!defined $child_pid) {
  my $now = iso_now();
  print "
=== Supervisor error $now: fork failed: $! ===
";
  $job->{lastRunStatus} = "failed";
  $job->{lastRunError} = "fork failed";
  $job->{updatedAt} = $now;
  write_json_atomic($job_path, $job);
  unlink $lock_path;
  exit 1;
}

if ($child_pid == 0) {
  chdir $workdir or die "Failed to chdir to $workdir: $!
";
  eval { setsid(); };
  exec { $command } $command, @args;
  die "Failed to exec $command: $!
";
}

if (defined($timeout) && $timeout > 0) {
  local $SIG{ALRM} = sub {
    $timed_out = 1;
    my $now = iso_now();
    print "
=== Timeout after $timeout seconds $now; sending SIGTERM ===
";
    kill 'TERM', -$child_pid;
    sleep 5;
    print "
=== Forcing SIGKILL $now ===
";
    kill 'KILL', -$child_pid;
  };
  alarm($timeout);
}

my $waited = waitpid($child_pid, 0);
my $status = $?;
alarm(0);

my $finished_at = iso_now();
my $duration_ms = int((time() - $t0) * 1000);
my $exit_code = ($status >> 8);
if ($timed_out) {
  $exit_code = 124;
}

my $final_status = "failed";
my $final_error = undef;
if ($timed_out) {
  $final_status = "failed";
  $final_error = "timeout";
} elsif ($waited != $child_pid) {
  $final_status = "failed";
  $final_error = "waitpid failed";
} elsif ($status == 0) {
  $final_status = "success";
} else {
  $final_status = "failed";
  $final_error = "exit code $exit_code";
}

$job->{lastRunStatus} = $final_status;
$job->{lastRunExitCode} = $exit_code;
$job->{lastRunError} = $final_error if defined $final_error;
$job->{updatedAt} = $finished_at;
write_json_atomic($job_path, $job);

append_jsonl("$runs_dir/$slug.jsonl", {
  runId => $run_id,
  scopeId => $scope_id,
  slug => $slug,
  startedAt => $started_at,
  finishedAt => $finished_at,
  durationMs => $duration_ms,
  status => $final_status,
  exitCode => $exit_code,
  error => $final_error,
  pid => $child_pid,
  logPath => $log_path,
});

unlink $lock_path;
print "
=== Finished $finished_at status=$final_status exitCode=$exit_code durationMs=$duration_ms ===
";
exit($exit_code);
`;
function ensureSupervisorScript() {
  ensureDir(SCHEDULER_DIR);
  writeFileSync(SUPERVISOR_PATH, SUPERVISOR_SCRIPT);
}
var WORKTREE_WRAPPER_SCRIPT = `#!/usr/bin/env bash
set -euo pipefail

# Usage: worktree-run.sh <scopeId> <slug> <baseRef> <wtBase> -- <command> [args...]
SCOPE_ID="\${1:-}"
SLUG="\${2:-}"
BASE_REF="\${3:-HEAD}"
WT_BASE="\${4:-}"
shift 4 || true
if [ "\${1:-}" = "--" ]; then shift; fi

if [ -z "$SCOPE_ID" ] || [ -z "$SLUG" ] || [ -z "$WT_BASE" ] || [ "$#" -eq 0 ]; then
  echo "worktree-run.sh: missing required arguments" >&2
  exit 2
fi

REPO_DIR="$PWD"
RUN_ID="\${OPENCODE_SCHEDULER_RUN_ID:-$(date +%s)-$$}"
GIT_ROOT="$(git -C "$REPO_DIR" rev-parse --show-toplevel 2>/dev/null || true)"
if [ -z "$GIT_ROOT" ]; then
  echo "worktree-run.sh: $REPO_DIR is not inside a git repository; running without isolation" >&2
  exec "$@"
fi

mkdir -p "$WT_BASE"
WT_DIR="$WT_BASE/\${SLUG}__\${RUN_ID}"
BRANCH="sched-\${SLUG}-\${RUN_ID}"
META="$WT_BASE/\${SLUG}__\${RUN_ID}.meta.json"
CREATED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

echo "=== [worktree] add $WT_DIR (branch $BRANCH from $BASE_REF) ==="
git -C "$GIT_ROOT" worktree add -b "$BRANCH" "$WT_DIR" "$BASE_REF"

cat > "$META" <<META_EOF
{"scopeId":"$SCOPE_ID","slug":"$SLUG","runId":"$RUN_ID","branch":"$BRANCH","gitRoot":"$GIT_ROOT","worktree":"$WT_DIR","baseRef":"$BASE_REF","createdAt":"$CREATED_AT"}
META_EOF

cd "$WT_DIR"
echo "=== [worktree] running in $WT_DIR ==="
exec "$@"
`;
function ensureWorktreeWrapper() {
  ensureDir(SCHEDULER_DIR);
  writeFileSync(WORKTREE_WRAPPER_PATH, WORKTREE_WRAPPER_SCRIPT, { mode: 493 });
}
function worktreeBaseDir(scopeId) {
  return join(WORKTREES_DIR, scopeId);
}
function listWorktreeMetas(filter) {
  const results = [];
  if (!existsSync(WORKTREES_DIR)) return results;
  const now = Date.now();
  const scopeDirs = filter?.scopeId ? [filter.scopeId] : readdirSync(WORKTREES_DIR);
  for (const scopeId of scopeDirs) {
    const base = worktreeBaseDir(scopeId);
    if (!existsSync(base)) continue;
    let entries;
    try {
      entries = readdirSync(base);
    } catch {
      continue;
    }
    for (const entry of entries) {
      if (!entry.endsWith(".meta.json")) continue;
      const metaPath = join(base, entry);
      let meta;
      try {
        meta = JSON.parse(readFileSync(metaPath, "utf-8"));
      } catch {
        continue;
      }
      if (!meta.worktree || !meta.slug) continue;
      if (filter?.slug && meta.slug !== filter.slug) continue;
      const createdMs = meta.createdAt ? Date.parse(meta.createdAt) : NaN;
      const ageHours = Number.isFinite(createdMs) ? (now - createdMs) / 36e5 : Infinity;
      results.push({
        scopeId: meta.scopeId ?? scopeId,
        slug: meta.slug,
        runId: meta.runId ?? "",
        branch: meta.branch ?? "",
        gitRoot: meta.gitRoot ?? "",
        worktree: meta.worktree,
        baseRef: meta.baseRef ?? "HEAD",
        createdAt: meta.createdAt ?? "",
        metaPath,
        exists: existsSync(meta.worktree),
        ageHours
      });
    }
  }
  return results.sort((a, b) => b.ageHours - a.ageHours);
}
function removeWorktree(meta, deleteBranch) {
  try {
    if (meta.gitRoot && existsSync(meta.gitRoot)) {
      if (existsSync(meta.worktree)) {
        try {
          execFileSync("git", ["-C", meta.gitRoot, "worktree", "remove", "--force", meta.worktree], {
            stdio: "ignore"
          });
        } catch {
          rmSync(meta.worktree, { recursive: true, force: true });
        }
      }
      try {
        execFileSync("git", ["-C", meta.gitRoot, "worktree", "prune"], { stdio: "ignore" });
      } catch {
      }
      if (deleteBranch && meta.branch) {
        try {
          execFileSync("git", ["-C", meta.gitRoot, "branch", "-D", meta.branch], { stdio: "ignore" });
        } catch {
        }
      }
    } else if (existsSync(meta.worktree)) {
      rmSync(meta.worktree, { recursive: true, force: true });
    }
    try {
      if (existsSync(meta.metaPath)) unlinkSync(meta.metaPath);
    } catch {
    }
    return { ok: true };
  } catch (error) {
    return { ok: false, error: error instanceof Error ? error.message : String(error) };
  }
}
function normalizeFormat(format) {
  return format === "json" ? "json" : "text";
}
function formatToolResult(format, result) {
  return format === "json" ? JSON.stringify(result, null, 2) : result.output;
}
function okResult(format, output, data) {
  return formatToolResult(format, { success: true, output, shouldContinue: false, data });
}
function errorResult(format, output, data) {
  return formatToolResult(format, { success: false, output, shouldContinue: true, data });
}
var SCHEDULED_JOB_BEST_PRACTICES_SKILL = {
  name: "scheduled-job-best-practices",
  description: "Patterns for resilient, non-interactive scheduled opencode jobs",
  suggestedPath: ".opencode/skill/scheduled-job-best-practices/SKILL.md",
  files: {
    "SKILL.md": `---
name: scheduled-job-best-practices
description: Patterns for resilient, non-interactive scheduled opencode jobs
---

## Use This Skill

Put this line at the very top of any scheduled job prompt:

@scheduled-job-best-practices

Then write your task below it.

## Core Principles

1. **No magic injection.** Do not assume placeholders like __TODAY__ exist. Compute runtime values using tools (bash) during the run.
2. **Non-interactive.** Scheduled jobs must not rely on QR codes, manual logins, or confirmation dialogs.
3. **Idempotent.** Make reruns safe (maintain a seen/state file; avoid duplicate messages).
4. **Observable.** Print a short summary at the end with status + outputs.
5. **Minimal side effects.** Write durable artifacts under outputs/ in the job workdir.

## Runtime Values: Dates

If you need local dates, compute them at runtime.

### macOS

~~~bash
TODAY="$(date +%F)"
TOMORROW="$(date -v+1d +%F)"
~~~

### Linux

~~~bash
TODAY="$(date +%F)"
TOMORROW="$(date -d 'tomorrow' +%F)"
~~~

### Portable snippet

~~~bash
if [ "$(uname)" = "Darwin" ]; then
  TODAY="$(date +%F)"
  TOMORROW="$(date -v+1d +%F)"
else
  TODAY="$(date +%F)"
  TOMORROW="$(date -d 'tomorrow' +%F)"
fi
~~~

If timezone matters, set TZ explicitly (example: TZ=America/Los_Angeles date +%F).

## Preflight Checklist

Before doing any expensive work:

- Confirm required tools are available (browser, network, etc).
- Confirm required env vars exist (source .env only if needed).
- If a dependency is missing/offline, stop early and emit a single concise reason.

## Notifications (Telegram)

Prefer the Telegram Bot API (non-interactive) over web.telegram.org.

## Output Contract

End every run with a compact summary:

- Status: success | skipped | failed
- Reason (1 line)
- Outputs written (paths)
- Notifications sent (message_id, chat_id) if applicable

## Idempotency Pattern

When notifying about \u201Cnew\u201D items (deals, alerts, etc.):

- Store a seen list in outputs/<job>/seen.json
- Only notify on items not in seen.json
- Update seen.json after sending
`
  }
};
var BUILTIN_SKILLS = {
  [SCHEDULED_JOB_BEST_PRACTICES_SKILL.name]: SCHEDULED_JOB_BEST_PRACTICES_SKILL
};
var SKILL_ALIASES = {
  "job-best-practices": SCHEDULED_JOB_BEST_PRACTICES_SKILL.name,
  "scheduled-jobs": SCHEDULED_JOB_BEST_PRACTICES_SKILL.name,
  scheduler: SCHEDULED_JOB_BEST_PRACTICES_SKILL.name
};
function normalizeSkillName(name) {
  const trimmed = (name ?? "").trim();
  if (!trimmed) return SCHEDULED_JOB_BEST_PRACTICES_SKILL.name;
  return SKILL_ALIASES[trimmed] ?? trimmed;
}
function getBuiltinSkill(name) {
  return BUILTIN_SKILLS[normalizeSkillName(name)];
}
function listBuiltinSkills() {
  return Object.values(BUILTIN_SKILLS);
}
function installBuiltinSkill(skill, rootDir, overwrite = false) {
  const installRoot = rootDir.trim();
  if (!installRoot) {
    throw new Error("Install directory cannot be empty.");
  }
  if (!existsSync(installRoot)) {
    throw new Error(`Directory not found: ${installRoot}`);
  }
  const relativeDir = dirname(skill.suggestedPath);
  const installDir = join(installRoot, relativeDir);
  ensureDir(installDir);
  const files = [];
  for (const [filename, content] of Object.entries(skill.files)) {
    const targetPath = join(installDir, filename);
    if (existsSync(targetPath) && !overwrite) {
      throw new Error(`File already exists: ${targetPath} (pass overwrite=true to replace)`);
    }
    writeFileSync(targetPath, `${content.trimEnd()}
`);
    files.push(targetPath);
  }
  return { directory: installDir, files };
}
function loadPackageInfo() {
  const fallback = { name: "opencode-scheduler", version: "unknown" };
  try {
    const packagePath = join(dirname(fileURLToPath(import.meta.url)), "..", "package.json");
    const raw = readFileSync(packagePath, "utf-8");
    const parsed = JSON.parse(raw);
    return {
      name: typeof parsed.name === "string" ? parsed.name : fallback.name,
      version: typeof parsed.version === "string" ? parsed.version : fallback.version
    };
  } catch {
    return fallback;
  }
}
function findOpencode() {
  const override = process.env.OPENCODE_SCHEDULER_OPENCODE_PATH?.trim();
  if (override) return override;
  try {
    const resolved = execSync("command -v opencode", {
      env: { ...process.env, PATH: getEnhancedPath() + ":" + (process.env.PATH ?? "") },
      stdio: ["ignore", "pipe", "ignore"]
    }).toString().trim();
    if (resolved) {
      if (resolved.includes("/")) return resolved;
      return "opencode";
    }
  } catch {
  }
  const paths = [
    "/opt/homebrew/bin/opencode",
    "/usr/local/bin/opencode",
    join(homedir(), ".opencode", "bin", "opencode")
  ];
  for (const p of paths) {
    if (existsSync(p)) {
      return p;
    }
  }
  return "opencode";
}
function getEnhancedPath() {
  const defaults = ["/opt/homebrew/bin", "/usr/local/bin", "/usr/bin", "/bin", "/usr/sbin", "/sbin"];
  const entries = [];
  const add = (value) => {
    if (!value) return;
    for (const segment of value.split(":")) {
      const trimmed = segment.trim();
      if (trimmed && !entries.includes(trimmed)) entries.push(trimmed);
    }
  };
  add(process.env.PATH);
  try {
    add(dirname(process.execPath));
  } catch {
  }
  for (const dir of defaults) add(dir);
  return entries.join(":");
}
function splitCronExpression(cron) {
  const parts = cron.trim().split(/\s+/);
  if (parts.length !== 5) {
    throw new Error(`Invalid cron: ${cron}`);
  }
  return parts;
}
function uniqueSorted(values) {
  return Array.from(new Set(values)).sort((a, b) => a - b);
}
function parseCronField(field, min, max, label, allowSundaySeven = false) {
  if (field === "*") return null;
  if (field.startsWith("*/")) {
    const step = parseInt(field.slice(2), 10);
    if (!Number.isFinite(step) || step <= 0) {
      throw new Error(`Invalid cron ${label} step: ${field}`);
    }
    const values = [];
    for (let value = min; value <= max; value += step) {
      values.push(value);
    }
    return values;
  }
  const parts = field.split(",");
  if (parts.length > 1) {
    const values = parts.map((part) => parseCronNumber(part, min, max, label, allowSundaySeven));
    return uniqueSorted(values);
  }
  if (/^\d+$/.test(field)) {
    return [parseCronNumber(field, min, max, label, allowSundaySeven)];
  }
  throw new Error(`Invalid cron ${label} field: ${field}`);
}
function parseCronNumber(value, min, max, label, allowSundaySeven) {
  const parsed = parseInt(value, 10);
  if (!Number.isFinite(parsed)) {
    throw new Error(`Invalid cron ${label} value: ${value}`);
  }
  const normalized = allowSundaySeven && parsed === 7 ? 0 : parsed;
  if (normalized < min || normalized > max) {
    throw new Error(`Invalid cron ${label} value: ${value}`);
  }
  return normalized;
}
function validateCronExpression(cron) {
  const [minute, hour, dayOfMonth, month, dayOfWeek] = splitCronExpression(cron);
  parseCronField(minute, 0, 59, "minute");
  parseCronField(hour, 0, 23, "hour");
  parseCronField(dayOfMonth, 1, 31, "day of month");
  parseCronField(month, 1, 12, "month");
  parseCronField(dayOfWeek, 0, 7, "day of week", true);
}
function expandLaunchdEntries(entries, key, values) {
  if (!values) return entries;
  const expanded = [];
  for (const entry of entries) {
    for (const value of values) {
      expanded.push({ ...entry, [key]: value });
    }
  }
  return expanded;
}
function buildLaunchdCalendars(minuteValues, hourValues, dayValues, monthValues, weekdayValues) {
  let entries = [{}];
  entries = expandLaunchdEntries(entries, "Minute", minuteValues);
  entries = expandLaunchdEntries(entries, "Hour", hourValues);
  entries = expandLaunchdEntries(entries, "Day", dayValues);
  entries = expandLaunchdEntries(entries, "Month", monthValues);
  entries = expandLaunchdEntries(entries, "Weekday", weekdayValues);
  return entries;
}
function cronToLaunchdCalendars(cron) {
  const [minute, hour, dayOfMonth, month, dayOfWeek] = splitCronExpression(cron);
  const minuteValues = parseCronField(minute, 0, 59, "minute");
  const hourValues = parseCronField(hour, 0, 23, "hour");
  const dayValues = parseCronField(dayOfMonth, 1, 31, "day of month");
  const monthValues = parseCronField(month, 1, 12, "month");
  const weekdayValues = parseCronField(dayOfWeek, 0, 7, "day of week", true);
  if (dayValues && weekdayValues) {
    return [
      ...buildLaunchdCalendars(minuteValues, hourValues, dayValues, monthValues, null),
      ...buildLaunchdCalendars(minuteValues, hourValues, null, monthValues, weekdayValues)
    ];
  }
  return buildLaunchdCalendars(minuteValues, hourValues, dayValues, monthValues, weekdayValues);
}
function escapePlistString(value) {
  return value.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}
function escapeSystemdArg(value) {
  return value.replace(/\\/g, "\\\\").replace(/"/g, '\\"');
}
function renderLaunchdCalendar(calendar) {
  return Object.entries(calendar).map(([key, value]) => `    <key>${key}</key>
    <integer>${value}</integer>`).join("\n");
}
var SYSTEMD_WEEKDAYS = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
function formatSystemdValue(value, size) {
  return value.toString().padStart(size, "0");
}
function cronToSystemdCalendars(cron) {
  const [minute, hour, dayOfMonth, month, dayOfWeek] = splitCronExpression(cron);
  const minuteValues = parseCronField(minute, 0, 59, "minute");
  const hourValues = parseCronField(hour, 0, 23, "hour");
  const dayValues = parseCronField(dayOfMonth, 1, 31, "day of month");
  const monthValues = parseCronField(month, 1, 12, "month");
  const weekdayValues = parseCronField(dayOfWeek, 0, 7, "day of week", true);
  const minutes = minuteValues ? minuteValues.map((value) => formatSystemdValue(value, 2)) : ["*"];
  const hours = hourValues ? hourValues.map((value) => formatSystemdValue(value, 2)) : ["*"];
  const days = dayValues ? dayValues.map((value) => formatSystemdValue(value, 2)) : ["*"];
  const months = monthValues ? monthValues.map((value) => formatSystemdValue(value, 2)) : ["*"];
  const weekdays = weekdayValues ? weekdayValues.map((value) => SYSTEMD_WEEKDAYS[value] ?? "*") : ["*"];
  const calendars = [];
  const buildCalendars = (domValues, dowValues) => {
    for (const minuteValue of minutes) {
      for (const hourValue of hours) {
        for (const domValue of domValues) {
          for (const monthValue of months) {
            for (const dowValue of dowValues) {
              calendars.push(`${dowValue} *-${monthValue}-${domValue} ${hourValue}:${minuteValue}:00`);
            }
          }
        }
      }
    }
  };
  if (dayValues && weekdayValues) {
    buildCalendars(days, ["*"]);
    buildCalendars(["*"], weekdays);
  } else {
    buildCalendars(days, weekdays);
  }
  return calendars;
}
var WINDOWS_WEEKDAYS = ["SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"];
var WINDOWS_MONTHS = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"];
function pad2(value) {
  return value.toString().padStart(2, "0");
}
function formatStartTime(hour, minute) {
  return `${pad2(hour)}:${pad2(minute)}`;
}
function windowsTaskBaseName(job) {
  const scopeId = job.scopeId || deriveScopeId(job.workdir || homedir());
  return `${WINDOWS_TASK_PREFIX}-${scopeId}-${job.slug}`;
}
function windowsTaskName(baseName, index, total) {
  const suffix = total > 1 ? `-${index + 1}` : "";
  return `${WINDOWS_TASK_ROOT}\\${baseName}${suffix}`;
}
function quoteWindowsArg(value) {
  if (!/[\s"]/u.test(value)) return value;
  return `"${value.replace(/"/g, '""')}"`;
}
function getWindowsInvocation(job) {
  const invocation = job.invocation ?? buildOpencodeArgs(job);
  return invocation;
}
function buildWindowsTaskCommand(job) {
  const invocation = getWindowsInvocation(job);
  return [invocation.command, ...invocation.args].map((arg) => quoteWindowsArg(arg)).join(" ");
}
function maybeStep(field) {
  const match = field.match(/^\*\/(\d+)$/);
  if (!match) return null;
  const step = parseInt(match[1], 10);
  return Number.isFinite(step) && step > 0 ? step : null;
}
function ensureWindowsRepresentable(cron) {
  const [minuteField, hourField, dayField, monthField, weekdayField] = splitCronExpression(cron);
  const minuteStep = maybeStep(minuteField);
  const hourStep = maybeStep(hourField);
  if (minuteField === "*" && hourField === "*" && dayField === "*" && monthField === "*" && weekdayField === "*") {
    return [{ schedule: "MINUTE", modifier: "1", startTime: "00:00" }];
  }
  if (minuteStep !== null && hourField === "*" && dayField === "*" && monthField === "*" && weekdayField === "*") {
    if (minuteStep > 1439) {
      throw new Error(
        `Windows Task Scheduler supports at most every 1439 minutes. Use ${minuteStep} with a smaller value or switch to an hourly/daily cron.`
      );
    }
    return [{ schedule: "MINUTE", modifier: String(minuteStep), startTime: "00:00" }];
  }
  const minuteValues = parseCronField(minuteField, 0, 59, "minute");
  const hourValues = parseCronField(hourField, 0, 23, "hour");
  const dayValues = parseCronField(dayField, 1, 31, "day of month");
  const monthValues = parseCronField(monthField, 1, 12, "month");
  const weekdayValues = parseCronField(weekdayField, 0, 7, "day of week", true);
  if (hourStep !== null && minuteValues && minuteValues.length === 1 && dayField === "*" && monthField === "*" && weekdayField === "*") {
    return [{ schedule: "HOURLY", modifier: String(hourStep), startTime: formatStartTime(0, minuteValues[0]) }];
  }
  if (!minuteValues || minuteValues.length === 0 || !hourValues || hourValues.length === 0) {
    throw new Error(
      "Windows Task Scheduler requires explicit minute and hour values for this cron expression. Use formats like '0 9 * * *', '30 8 * * 1', '*/15 * * * *', or '0 */6 * * *'."
    );
  }
  if (monthValues && weekdayValues) {
    throw new Error(
      "Windows Task Scheduler cannot combine specific months with day-of-week constraints in cron. Split this into multiple jobs (for example: one monthly job and one weekly job)."
    );
  }
  if (monthValues && !dayValues) {
    throw new Error(
      "Windows Task Scheduler cannot represent 'every day in selected months'. Use explicit day-of-month values (for example '0 9 1,15 1,7 *') or create separate jobs."
    );
  }
  const plans = [];
  for (const minute of minuteValues) {
    for (const hour of hourValues) {
      const startTime = formatStartTime(hour, minute);
      if (dayValues && weekdayValues) {
        plans.push({
          schedule: "MONTHLY",
          days: dayValues.join(","),
          months: monthValues ? monthValues.map((value) => WINDOWS_MONTHS[value - 1]).join(",") : void 0,
          startTime
        });
        plans.push({
          schedule: "WEEKLY",
          weekdays: weekdayValues.map((value) => WINDOWS_WEEKDAYS[value]).join(","),
          startTime
        });
      } else if (weekdayValues) {
        plans.push({
          schedule: "WEEKLY",
          weekdays: weekdayValues.map((value) => WINDOWS_WEEKDAYS[value]).join(","),
          startTime
        });
      } else if (dayValues) {
        plans.push({
          schedule: "MONTHLY",
          days: dayValues.join(","),
          months: monthValues ? monthValues.map((value) => WINDOWS_MONTHS[value - 1]).join(",") : void 0,
          startTime
        });
      } else if (monthValues) {
        throw new Error(
          "Windows Task Scheduler cannot represent month-only cron constraints without day-of-month. Use explicit days or create separate jobs."
        );
      } else {
        plans.push({ schedule: "DAILY", startTime });
      }
    }
  }
  return plans;
}
function cronToWindowsTaskDefinitions(job) {
  const plans = ensureWindowsRepresentable(job.schedule);
  const baseName = windowsTaskBaseName(job);
  const command = buildWindowsTaskCommand(job);
  return plans.map((plan, index) => {
    const args = ["/Create", "/F", "/TN", windowsTaskName(baseName, index, plans.length), "/TR", command, "/SC", plan.schedule];
    if (plan.modifier) {
      args.push("/MO", plan.modifier);
    }
    if (plan.weekdays) {
      args.push("/D", plan.weekdays);
    }
    if (plan.days) {
      args.push("/D", plan.days);
    }
    if (plan.months) {
      args.push("/M", plan.months);
    }
    args.push("/ST", plan.startTime);
    return { name: windowsTaskName(baseName, index, plans.length), args };
  });
}
function createLaunchdPlist(job) {
  const scopeId = job.scopeId || deriveScopeId(job.workdir || homedir());
  const label = `${LAUNCHD_PREFIX}.${scopeId}.${job.slug}`;
  const logFilePath = scopedLogPath(scopeId, job.slug);
  const jobPath = jobFilePath(scopeId, job.slug);
  const calendars = cronToLaunchdCalendars(job.schedule);
  const calendarXml = calendars.length === 1 ? `  <dict>
${renderLaunchdCalendar(calendars[0])}
  </dict>` : `  <array>
${calendars.map((calendar) => `  <dict>
${renderLaunchdCalendar(calendar)}
  </dict>`).join("\n")}
  </array>`;
  const programArgumentsXml = [
    `    <string>${escapePlistString("/usr/bin/perl")}</string>`,
    `    <string>${escapePlistString(SUPERVISOR_PATH)}</string>`,
    `    <string>${escapePlistString(jobPath)}</string>`
  ].join("\n");
  const workdir = job.workdir || homedir();
  const enhancedPath = getEnhancedPath();
  return `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>${label}</string>
  
  <key>WorkingDirectory</key>
  <string>${escapePlistString(workdir)}</string>
  
  <key>EnvironmentVariables</key>
  <dict>
    <key>PATH</key>
    <string>${enhancedPath}</string>
  </dict>
  
  <key>ProgramArguments</key>
  <array>
${programArgumentsXml}
  </array>
  
  <key>StartCalendarInterval</key>
${calendarXml}
  
  <key>StandardOutPath</key>
  <string>${logFilePath}</string>
  
  <key>StandardErrorPath</key>
  <string>${logFilePath}</string>
  
  <key>RunAtLoad</key>
  <false/>
</dict>
</plist>`;
}
function installLaunchdJob(job) {
  ensureDir(LAUNCH_AGENTS_DIR);
  ensureDir(LOGS_DIR);
  const scopeId = job.scopeId || deriveScopeId(job.workdir || homedir());
  ensureDir(scopeLogsDir(scopeId));
  ensureSupervisorScript();
  const legacyLabel = `${LAUNCHD_PREFIX}.${job.slug}`;
  const legacyPlistPath = join(LAUNCH_AGENTS_DIR, `${legacyLabel}.plist`);
  const label = `${LAUNCHD_PREFIX}.${scopeId}.${job.slug}`;
  const plistPath = join(LAUNCH_AGENTS_DIR, `${label}.plist`);
  try {
    execSync(`launchctl unload "${plistPath}" 2>/dev/null`, { stdio: "ignore" });
  } catch {
  }
  if (existsSync(legacyPlistPath)) {
    try {
      execSync(`launchctl unload "${legacyPlistPath}" 2>/dev/null`, { stdio: "ignore" });
    } catch {
    }
  }
  const plist = createLaunchdPlist(job);
  writeFileSync(plistPath, plist);
  execSync(`launchctl load "${plistPath}"`);
}
function uninstallLaunchdJob(job) {
  const scopeId = job.scopeId || deriveScopeId(job.workdir || homedir());
  const scopedLabel = `${LAUNCHD_PREFIX}.${scopeId}.${job.slug}`;
  const scopedPlistPath = join(LAUNCH_AGENTS_DIR, `${scopedLabel}.plist`);
  const legacyLabel = `${LAUNCHD_PREFIX}.${job.slug}`;
  const legacyPlistPath = join(LAUNCH_AGENTS_DIR, `${legacyLabel}.plist`);
  for (const plistPath of [scopedPlistPath, legacyPlistPath]) {
    if (!existsSync(plistPath)) continue;
    try {
      execSync(`launchctl unload "${plistPath}"`, { stdio: "ignore" });
    } catch {
    }
    try {
      unlinkSync(plistPath);
    } catch {
    }
  }
}
function createSystemdService(job) {
  const scopeId = job.scopeId || deriveScopeId(job.workdir || homedir());
  const logFilePath = scopedLogPath(scopeId, job.slug);
  const jobPath = jobFilePath(scopeId, job.slug);
  const workdir = job.workdir || homedir();
  const enhancedPath = getEnhancedPath();
  const execStart = ["/usr/bin/perl", SUPERVISOR_PATH, jobPath].map((arg) => `"${escapeSystemdArg(arg)}"`).join(" ");
  return `[Unit]
Description=OpenCode Job: ${job.name}

[Service]
Type=oneshot
WorkingDirectory=${workdir}
Environment="PATH=${enhancedPath}"
ExecStart=${execStart}
StandardOutput=append:${logFilePath}
StandardError=append:${logFilePath}

[Install]
WantedBy=default.target
`;
}
function createSystemdTimer(job) {
  const calendars = cronToSystemdCalendars(job.schedule);
  const calendarLines = calendars.map((calendar) => `OnCalendar=${calendar}`).join("\n");
  return `[Unit]
Description=Timer for OpenCode Job: ${job.name}

[Timer]
${calendarLines}
Persistent=true

[Install]
WantedBy=timers.target
`;
}
function installSystemdJob(job) {
  ensureDir(SYSTEMD_USER_DIR);
  ensureDir(LOGS_DIR);
  const scopeId = job.scopeId || deriveScopeId(job.workdir || homedir());
  ensureDir(scopeLogsDir(scopeId));
  ensureSupervisorScript();
  const servicePath = join(SYSTEMD_USER_DIR, `opencode-job-${scopeId}-${job.slug}.service`);
  const timerPath = join(SYSTEMD_USER_DIR, `opencode-job-${scopeId}-${job.slug}.timer`);
  try {
    execSync(`systemctl --user stop opencode-job-${job.slug}.timer`, { stdio: "ignore" });
    execSync(`systemctl --user disable opencode-job-${job.slug}.timer`, { stdio: "ignore" });
  } catch {
  }
  writeFileSync(servicePath, createSystemdService(job));
  writeFileSync(timerPath, createSystemdTimer(job));
  execSync("systemctl --user daemon-reload");
  execSync(`systemctl --user enable opencode-job-${scopeId}-${job.slug}.timer`);
  execSync(`systemctl --user start opencode-job-${scopeId}-${job.slug}.timer`);
}
function uninstallSystemdJob(job) {
  const scopeId = job.scopeId || deriveScopeId(job.workdir || homedir());
  const scopedTimerUnit = `opencode-job-${scopeId}-${job.slug}.timer`;
  const legacyTimerUnit = `opencode-job-${job.slug}.timer`;
  for (const timerUnit of [scopedTimerUnit, legacyTimerUnit]) {
    try {
      execSync(`systemctl --user stop ${timerUnit}`, { stdio: "ignore" });
      execSync(`systemctl --user disable ${timerUnit}`, { stdio: "ignore" });
    } catch {
    }
  }
  const scopedServicePath = join(SYSTEMD_USER_DIR, `opencode-job-${scopeId}-${job.slug}.service`);
  const scopedTimerPath = join(SYSTEMD_USER_DIR, `opencode-job-${scopeId}-${job.slug}.timer`);
  const legacyServicePath = join(SYSTEMD_USER_DIR, `opencode-job-${job.slug}.service`);
  const legacyTimerPath = join(SYSTEMD_USER_DIR, `opencode-job-${job.slug}.timer`);
  for (const p of [scopedServicePath, scopedTimerPath, legacyServicePath, legacyTimerPath]) {
    if (existsSync(p)) {
      try {
        unlinkSync(p);
      } catch {
      }
    }
  }
  try {
    execSync("systemctl --user daemon-reload", { stdio: "ignore" });
  } catch {
  }
}
function installWindowsJob(job) {
  uninstallWindowsJob(job);
  const taskDefinitions = cronToWindowsTaskDefinitions(job);
  for (const task of taskDefinitions) {
    execFileSync("schtasks", task.args, { stdio: "ignore" });
  }
}
function uninstallWindowsJob(job) {
  const candidates = /* @__PURE__ */ new Set();
  const scopedBase = windowsTaskBaseName(job);
  const legacyBase = `${WINDOWS_TASK_PREFIX}-${job.slug}`;
  for (let i = 0; i < 64; i += 1) {
    const suffix = i === 0 ? "" : `-${i + 1}`;
    candidates.add(`${WINDOWS_TASK_ROOT}\\${scopedBase}${suffix}`);
    candidates.add(`${WINDOWS_TASK_ROOT}\\${legacyBase}${suffix}`);
  }
  for (const taskName of candidates) {
    try {
      execFileSync("schtasks", ["/Delete", "/TN", taskName, "/F"], { stdio: "ignore" });
    } catch {
    }
  }
}
function isCommandAvailable(command) {
  try {
    execSync(`command -v ${command}`, {
      stdio: "ignore",
      env: buildRunEnvironment()
    });
    return true;
  } catch {
    return false;
  }
}
function isSystemdUserAvailable() {
  if (!IS_LINUX) return false;
  if (!isCommandAvailable("systemctl")) return false;
  try {
    execSync("systemctl --user show-environment", {
      stdio: "ignore",
      env: buildRunEnvironment()
    });
    return true;
  } catch {
    return false;
  }
}
function isCronAvailable() {
  if (IS_WINDOWS) return false;
  return isCommandAvailable("crontab");
}
function cronBlockId(job) {
  const scopeId = job.scopeId || deriveScopeId(job.workdir || homedir());
  return `${scopeId}:${job.slug}`;
}
function cronLegacyBlockId(job) {
  return `legacy:${job.slug}`;
}
function cronBlockStart(id) {
  return `# BEGIN ${CRON_MANAGED_PREFIX} ${id}`;
}
function cronBlockEnd(id) {
  return `# END ${CRON_MANAGED_PREFIX} ${id}`;
}
function shellEscapeDoubleQuoted(value) {
  return value.replace(/(["\\$`])/g, "\\$1");
}
function readUserCrontab() {
  try {
    return execFileSync("crontab", ["-l"], { encoding: "utf-8" });
  } catch (error) {
    const status = typeof error === "object" && error !== null ? error.status : void 0;
    const stderrValue = typeof error === "object" && error !== null && "stderr" in error ? error.stderr : void 0;
    const stderr = Buffer.isBuffer(stderrValue) ? stderrValue.toString("utf-8") : stderrValue ?? "";
    const noCrontab = status === 1 && (!stderr.trim() || /no crontab/i.test(stderr));
    if (noCrontab) return "";
    throw error;
  }
}
function writeUserCrontab(content) {
  const normalized = content.trim();
  const input = normalized ? `${normalized}
` : "";
  execFileSync("crontab", ["-"], { input });
}
function stripManagedCronBlocks(content, blockIds) {
  const lines = content ? content.split(/\r?\n/) : [];
  const retained = [];
  const prefix = `# BEGIN ${CRON_MANAGED_PREFIX} `;
  const endPrefix = `# END ${CRON_MANAGED_PREFIX} `;
  let removed = 0;
  for (let index = 0; index < lines.length; ) {
    const line = lines[index];
    if (!line.startsWith(prefix)) {
      retained.push(line);
      index += 1;
      continue;
    }
    const id = line.slice(prefix.length).trim();
    let endIndex = lines.length - 1;
    for (let probe = index + 1; probe < lines.length; probe += 1) {
      if (lines[probe] === `${endPrefix}${id}`) {
        endIndex = probe;
        break;
      }
    }
    if (blockIds.has(id)) {
      removed += 1;
    } else {
      for (let keep = index; keep <= endIndex; keep += 1) {
        retained.push(lines[keep]);
      }
    }
    index = endIndex + 1;
  }
  return {
    content: retained.join("\n").replace(/\n{3,}/g, "\n\n").trimEnd(),
    removed
  };
}
function createCronEntry(job) {
  const scopeId = job.scopeId || deriveScopeId(job.workdir || homedir());
  const jobPath = jobFilePath(scopeId, job.slug);
  const logFilePath = scopedLogPath(scopeId, job.slug);
  const escapedSupervisor = shellEscapeDoubleQuoted(SUPERVISOR_PATH);
  const escapedJobPath = shellEscapeDoubleQuoted(jobPath);
  const escapedLogPath = shellEscapeDoubleQuoted(logFilePath);
  const escapedPath = shellEscapeDoubleQuoted(getEnhancedPath());
  return `${job.schedule} PATH="${escapedPath}" /usr/bin/perl "${escapedSupervisor}" "${escapedJobPath}" >> "${escapedLogPath}" 2>&1`;
}
function installCronJob(job) {
  if (!isCronAvailable()) {
    throw new Error("cron backend is unavailable: `crontab` command not found.");
  }
  ensureDir(LOGS_DIR);
  const scopeId = job.scopeId || deriveScopeId(job.workdir || homedir());
  ensureDir(scopeLogsDir(scopeId));
  ensureSupervisorScript();
  const blockId = cronBlockId(job);
  const current = readUserCrontab();
  const stripped = stripManagedCronBlocks(current, /* @__PURE__ */ new Set([blockId, cronLegacyBlockId(job)]));
  const block = [cronBlockStart(blockId), createCronEntry(job), cronBlockEnd(blockId)].join("\n");
  const next = [stripped.content.trim(), block].filter(Boolean).join("\n\n");
  writeUserCrontab(next);
}
function uninstallCronJob(job) {
  if (!isCronAvailable()) return;
  const current = readUserCrontab();
  const stripped = stripManagedCronBlocks(current, /* @__PURE__ */ new Set([cronBlockId(job), cronLegacyBlockId(job)]));
  if (stripped.removed === 0) return;
  writeUserCrontab(stripped.content);
}
function resolveSchedulerBackend() {
  if (IS_MAC) return "launchd";
  if (IS_WINDOWS) return "schtasks";
  if (isSystemdUserAvailable()) return "systemd";
  if (isCronAvailable()) return "cron";
  if (IS_LINUX) {
    throw new Error(
      "No supported scheduler backend found: systemd --user is unavailable and `crontab` is not installed."
    );
  }
  throw new Error(
    `Unsupported platform: ${platform()}. Supported platforms: macOS (launchd), Linux (systemd or cron), Windows, and POSIX systems with cron.`
  );
}
function installJob(job) {
  const backend = resolveSchedulerBackend();
  if (backend === "launchd") {
    installLaunchdJob(job);
  } else if (backend === "systemd") {
    try {
      installSystemdJob(job);
    } catch (error) {
      if (!isCronAvailable()) {
        throw error;
      }
      installCronJob(job);
      return "cron";
    }
  } else if (backend === "schtasks") {
    installWindowsJob(job);
  } else {
    installCronJob(job);
  }
  return backend;
}
function uninstallJob(job) {
  if (IS_MAC) {
    uninstallLaunchdJob(job);
    return;
  }
  if (IS_WINDOWS) {
    uninstallWindowsJob(job);
    return;
  }
  if (IS_LINUX) {
    uninstallSystemdJob(job);
    uninstallCronJob(job);
    return;
  }
  uninstallCronJob(job);
}
function ensureScopeStorage(scopeId) {
  ensureDir(SCHEDULER_DIR);
  ensureDir(SCOPES_DIR);
  ensureDir(scopeJobsDir(scopeId));
  ensureDir(scopeLocksDir(scopeId));
  ensureDir(scopeRunsDir(scopeId));
  ensureDir(scopeLogsDir(scopeId));
}
function loadScopedJob(scopeId, slug) {
  ensureScopeStorage(scopeId);
  const path = jobFilePath(scopeId, slug);
  if (!existsSync(path)) return null;
  try {
    return normalizeJob(JSON.parse(readFileSync(path, "utf-8")));
  } catch {
    return null;
  }
}
function loadAllScopedJobs(scopeId) {
  ensureScopeStorage(scopeId);
  const files = readdirSync(scopeJobsDir(scopeId)).filter((f) => f.endsWith(".json"));
  return files.map((f) => {
    try {
      return normalizeJob(JSON.parse(readFileSync(join(scopeJobsDir(scopeId), f), "utf-8")));
    } catch {
      return null;
    }
  }).filter(Boolean);
}
function listScopeIds() {
  ensureDir(SCOPES_DIR);
  try {
    return readdirSync(SCOPES_DIR).filter((name) => {
      try {
        return existsSync(scopeDir(name));
      } catch {
        return false;
      }
    }).sort();
  } catch {
    return [];
  }
}
function loadAllJobsAcrossScopes() {
  const scopeIds = listScopeIds();
  const out = [];
  for (const scopeId of scopeIds) {
    out.push(...loadAllScopedJobs(scopeId));
  }
  return out;
}
function loadLegacyJob(slug) {
  ensureDir(LEGACY_JOBS_DIR);
  const path = join(LEGACY_JOBS_DIR, `${slug}.json`);
  if (!existsSync(path)) return null;
  try {
    return normalizeJob(JSON.parse(readFileSync(path, "utf-8")));
  } catch {
    return null;
  }
}
function loadAllLegacyJobs() {
  ensureDir(LEGACY_JOBS_DIR);
  const files = readdirSync(LEGACY_JOBS_DIR).filter((f) => f.endsWith(".json"));
  return files.map((f) => {
    try {
      return normalizeJob(JSON.parse(readFileSync(join(LEGACY_JOBS_DIR, f), "utf-8")));
    } catch {
      return null;
    }
  }).filter(Boolean);
}
function saveJob(job) {
  const scopeId = job.scopeId || deriveScopeId(job.workdir || homedir());
  const normalizedJob = { ...job, scopeId };
  ensureScopeStorage(scopeId);
  const path = jobFilePath(scopeId, normalizedJob.slug);
  writeFileSync(path, JSON.stringify(sanitizeJob(normalizedJob), null, 2));
}
function deleteJobFile(job) {
  const scopeId = job.scopeId || deriveScopeId(job.workdir || homedir());
  const path = jobFilePath(scopeId, job.slug);
  if (existsSync(path)) {
    unlinkSync(path);
  }
}
function listDirectoryFiles(dir, options) {
  if (!existsSync(dir)) return [];
  try {
    const entries = readdirSync(dir, { withFileTypes: true });
    return entries.filter((entry) => entry.isFile()).map((entry) => entry.name).filter((name) => options?.prefix ? name.startsWith(options.prefix) : true).filter((name) => options?.suffix ? name.endsWith(options.suffix) : true).map((name) => join(dir, name)).sort();
  } catch {
    return [];
  }
}
function listDirectoryNames(dir) {
  if (!existsSync(dir)) return [];
  try {
    return readdirSync(dir, { withFileTypes: true }).filter((entry) => entry.isDirectory()).map((entry) => entry.name).sort();
  } catch {
    return [];
  }
}
function uniquePaths(paths) {
  return Array.from(new Set(paths)).sort();
}
function buildGlobalCleanupPlan(includeHistory) {
  const scopeIds = listScopeIds();
  const scopedJobDefinitionPaths = scopeIds.flatMap((scopeId) => listDirectoryFiles(scopeJobsDir(scopeId), { suffix: ".json" }));
  const lockPaths = scopeIds.flatMap((scopeId) => listDirectoryFiles(scopeLocksDir(scopeId), { suffix: ".json" }));
  const runHistoryPaths = includeHistory ? scopeIds.flatMap((scopeId) => listDirectoryFiles(scopeRunsDir(scopeId), { suffix: ".jsonl" })) : [];
  const schedulerLogsRoot = join(LOGS_DIR, "scheduler");
  const logScopeIds = listDirectoryNames(schedulerLogsRoot);
  const logPaths = includeHistory ? logScopeIds.flatMap((scopeId) => listDirectoryFiles(join(schedulerLogsRoot, scopeId), { suffix: ".log" })) : [];
  const launchdPaths = IS_MAC ? listDirectoryFiles(LAUNCH_AGENTS_DIR, { prefix: `${LAUNCHD_PREFIX}.`, suffix: ".plist" }) : [];
  const systemdPaths = IS_LINUX ? [
    ...listDirectoryFiles(SYSTEMD_USER_DIR, { prefix: "opencode-job-", suffix: ".service" }),
    ...listDirectoryFiles(SYSTEMD_USER_DIR, { prefix: "opencode-job-", suffix: ".timer" })
  ] : [];
  const jobsToUninstall = [...loadAllJobsAcrossScopes(), ...loadAllLegacyJobs()];
  return {
    scopeIds,
    jobsToUninstall,
    scopedJobDefinitionPaths: uniquePaths(scopedJobDefinitionPaths),
    legacyJobDefinitionPaths: listDirectoryFiles(LEGACY_JOBS_DIR, { suffix: ".json" }),
    lockPaths: uniquePaths(lockPaths),
    runHistoryPaths: uniquePaths(runHistoryPaths),
    logPaths: uniquePaths(logPaths),
    launchdPaths: uniquePaths(launchdPaths),
    systemdPaths: uniquePaths(systemdPaths)
  };
}
function removePaths(paths, errors) {
  const removed = [];
  for (const path of uniquePaths(paths)) {
    if (!existsSync(path)) continue;
    try {
      rmSync(path, { recursive: true, force: true });
      removed.push(path);
    } catch (error) {
      const msg = error instanceof Error ? error.message : String(error);
      errors.push(`Failed to remove ${path}: ${msg}`);
    }
  }
  return removed;
}
function executeGlobalCleanup(plan, options) {
  const errors = [];
  const dryRun = options.dryRun;
  if (!dryRun) {
    for (const job of plan.jobsToUninstall) {
      try {
        uninstallJob(job);
      } catch (error) {
        const msg = error instanceof Error ? error.message : String(error);
        errors.push(`Failed to uninstall scheduler entry for ${job.slug}: ${msg}`);
      }
    }
  }
  const removeOrPreview = (paths) => {
    if (dryRun) return uniquePaths(paths).filter((path) => existsSync(path));
    return removePaths(paths, errors);
  };
  const removed = {
    scopedJobDefinitions: removeOrPreview(plan.scopedJobDefinitionPaths),
    legacyJobDefinitions: removeOrPreview(plan.legacyJobDefinitionPaths),
    locks: removeOrPreview(plan.lockPaths),
    runHistory: options.includeHistory ? removeOrPreview(plan.runHistoryPaths) : [],
    logs: options.includeHistory ? removeOrPreview(plan.logPaths) : [],
    launchdUnits: removeOrPreview(plan.launchdPaths),
    systemdUnits: removeOrPreview(plan.systemdPaths)
  };
  return {
    dryRun,
    includeHistory: options.includeHistory,
    removed,
    errors
  };
}
function formatCleanupLine(label, count, location) {
  return `- ${label}: ${count} (${location})`;
}
function formatGlobalCleanupOutput(execution) {
  const mode = execution.dryRun ? "DRY RUN (no files deleted)" : "EXECUTED";
  const lines = [
    `Global scheduler cleanup: ${mode}`,
    "",
    formatCleanupLine("Scoped job definitions", execution.removed.scopedJobDefinitions.length, `${SCOPES_DIR}/*/jobs`),
    formatCleanupLine("Legacy job definitions", execution.removed.legacyJobDefinitions.length, LEGACY_JOBS_DIR),
    formatCleanupLine("Lock files", execution.removed.locks.length, `${SCOPES_DIR}/*/locks`)
  ];
  if (execution.includeHistory) {
    lines.push(formatCleanupLine("Run history", execution.removed.runHistory.length, `${SCOPES_DIR}/*/runs`));
    lines.push(formatCleanupLine("Logs", execution.removed.logs.length, `${LOGS_DIR}/scheduler/*`));
  } else {
    lines.push("- Run history: skipped (set includeHistory=true)");
    lines.push("- Logs: skipped (set includeHistory=true)");
  }
  if (IS_MAC) {
    lines.push(formatCleanupLine("launchd plists", execution.removed.launchdUnits.length, LAUNCH_AGENTS_DIR));
  }
  if (IS_LINUX) {
    lines.push(formatCleanupLine("systemd units", execution.removed.systemdUnits.length, SYSTEMD_USER_DIR));
  }
  if (execution.errors.length > 0) {
    lines.push("");
    lines.push("Errors:");
    for (const error of execution.errors) {
      lines.push(`- ${error}`);
    }
  }
  if (execution.dryRun) {
    lines.push("");
    lines.push("Re-run with confirm=true to apply this cleanup.");
  }
  return lines.join("\n");
}
function normalizeAttachUrl(attachUrl) {
  if (attachUrl === void 0) return void 0;
  const trimmed = attachUrl.trim();
  if (!trimmed) return void 0;
  try {
    new URL(trimmed);
  } catch {
    throw new Error(`Invalid attach URL: ${attachUrl}`);
  }
  return trimmed;
}
function isRecord(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
function normalizeRunFormat(value) {
  if (typeof value !== "string") return void 0;
  const trimmed = value.trim();
  if (trimmed === "json") return "json";
  if (trimmed === "default") return "default";
  return void 0;
}
function parseRunFormatInput(value) {
  if (value === void 0) return void 0;
  if (typeof value === "string" && !value.trim()) return void 0;
  const normalized = normalizeRunFormat(value);
  if (normalized) return normalized;
  throw new Error(`Invalid runFormat: ${String(value)} (expected: default | json)`);
}
function normalizeRunSpec(run) {
  const normalized = { ...run };
  if (typeof normalized.prompt === "string") {
    const trimmed = normalized.prompt.trim();
    normalized.prompt = trimmed ? trimmed : void 0;
  }
  if (typeof normalized.command === "string") {
    const trimmed = normalized.command.trim();
    normalized.command = trimmed ? trimmed : void 0;
  }
  if (typeof normalized.arguments === "string") {
    const trimmed = normalized.arguments.trim();
    normalized.arguments = trimmed ? trimmed : void 0;
  }
  if (Array.isArray(normalized.files)) {
    const files = normalized.files.map((file) => String(file).trim()).filter(Boolean);
    normalized.files = files.length ? files : void 0;
  }
  if (typeof normalized.agent === "string") {
    const trimmed = normalized.agent.trim();
    normalized.agent = trimmed ? trimmed : void 0;
  }
  if (typeof normalized.model === "string") {
    const trimmed = normalized.model.trim();
    normalized.model = trimmed ? trimmed : void 0;
  }
  if (typeof normalized.variant === "string") {
    const trimmed = normalized.variant.trim();
    normalized.variant = trimmed ? trimmed : void 0;
  }
  if (typeof normalized.title === "string") {
    const trimmed = normalized.title.trim();
    normalized.title = trimmed ? trimmed : void 0;
  }
  if (normalized.share !== true) {
    normalized.share = void 0;
  }
  if (normalized.continue !== true) {
    normalized.continue = void 0;
  }
  if (typeof normalized.session === "string") {
    const trimmed = normalized.session.trim();
    normalized.session = trimmed ? trimmed : void 0;
  }
  if (normalized.runFormat !== "json") {
    normalized.runFormat = normalized.runFormat === "default" ? "default" : void 0;
  }
  if (typeof normalized.attachUrl === "string") {
    const trimmed = normalized.attachUrl.trim();
    normalized.attachUrl = trimmed ? trimmed : void 0;
  }
  if (typeof normalized.port === "number" && Number.isFinite(normalized.port)) {
    normalized.port = Math.floor(normalized.port);
    if (normalized.port <= 0) normalized.port = void 0;
  } else {
    normalized.port = void 0;
  }
  return normalized;
}
function validateRunSpec(run) {
  const hasPrompt = typeof run.prompt === "string" && run.prompt.trim().length > 0;
  const hasCommand = typeof run.command === "string" && run.command.trim().length > 0;
  if (!hasPrompt && !hasCommand) {
    throw new Error("Job must have either run.prompt or run.command");
  }
  if (hasPrompt && hasCommand) {
    throw new Error("Job cannot specify both run.prompt and run.command");
  }
  if (hasCommand && run.arguments !== void 0 && typeof run.arguments !== "string") {
    throw new Error("run.arguments must be a string");
  }
  if (run.attachUrl !== void 0) {
    normalizeAttachUrl(run.attachUrl);
  }
  if (run.port !== void 0) {
    if (!Number.isFinite(run.port) || run.port <= 0) {
      throw new Error("run.port must be a positive integer");
    }
  }
  if (run.runFormat !== void 0 && run.runFormat !== "default" && run.runFormat !== "json") {
    throw new Error("run.runFormat must be 'default' or 'json'");
  }
}
function getJobRun(job) {
  if (job.run) {
    return job.run;
  }
  const fallbackPrompt = (job.prompt ?? "").trim();
  if (!fallbackPrompt) {
    throw new Error(`Job "${job.slug}" is missing a prompt. Update the job to include run.prompt or prompt.`);
  }
  return {
    prompt: fallbackPrompt,
    attachUrl: job.attachUrl
  };
}
function sanitizeJob(job) {
  const sanitized = { ...job };
  if (typeof sanitized.workdir === "string") {
    const trimmed = sanitized.workdir.trim();
    sanitized.workdir = trimmed ? trimmed : void 0;
  }
  if (typeof sanitized.scopeId === "string") {
    const trimmed = sanitized.scopeId.trim();
    sanitized.scopeId = trimmed ? trimmed : void 0;
  }
  if (!sanitized.scopeId) {
    sanitized.scopeId = deriveScopeId(sanitized.workdir || homedir());
  }
  if (sanitized.timeoutSeconds !== void 0) {
    const n = sanitized.timeoutSeconds;
    if (typeof n !== "number" || !Number.isFinite(n) || n < 0 || Math.floor(n) !== n) {
      throw new Error("timeoutSeconds must be a non-negative integer");
    }
  }
  if (sanitized.invocation !== void 0) {
    const inv = sanitized.invocation;
    if (!inv || typeof inv !== "object") {
      throw new Error("invocation must be an object");
    }
    const rec = inv;
    if (typeof rec.command !== "string" || !rec.command.trim()) {
      throw new Error("invocation.command must be a non-empty string");
    }
    if (!Array.isArray(rec.args)) {
      throw new Error("invocation.args must be an array");
    }
    sanitized.invocation = {
      command: rec.command,
      args: rec.args.map((v) => String(v))
    };
  }
  if (sanitized.run) {
    const normalized = normalizeRunSpec(sanitized.run);
    validateRunSpec(normalized);
    sanitized.run = normalized;
  }
  if (sanitized.attachUrl !== void 0) {
    sanitized.attachUrl = normalizeAttachUrl(sanitized.attachUrl);
  }
  if (sanitized.prompt !== void 0) {
    const trimmed = sanitized.prompt.trim();
    sanitized.prompt = trimmed ? trimmed : void 0;
  }
  return sanitized;
}
function normalizeJobInvocation(raw) {
  if (!isRecord(raw)) return void 0;
  if (typeof raw.command !== "string") return void 0;
  if (!Array.isArray(raw.args)) return void 0;
  const command = raw.command.trim();
  if (!command) return void 0;
  return { command, args: raw.args.map((v) => String(v)) };
}
function normalizeJobRun(raw) {
  if (!isRecord(raw)) return void 0;
  const run = {};
  if (typeof raw.prompt === "string") run.prompt = raw.prompt;
  if (typeof raw.command === "string") run.command = raw.command;
  if (typeof raw.arguments === "string") run.arguments = raw.arguments;
  if (Array.isArray(raw.files)) {
    run.files = raw.files.map((file) => String(file));
  }
  if (typeof raw.agent === "string") run.agent = raw.agent;
  if (typeof raw.model === "string") run.model = raw.model;
  if (typeof raw.variant === "string") run.variant = raw.variant;
  if (typeof raw.title === "string") run.title = raw.title;
  if (typeof raw.share === "boolean") run.share = raw.share;
  if (typeof raw.continue === "boolean") run.continue = raw.continue;
  if (typeof raw.session === "string") run.session = raw.session;
  const runFormat = normalizeRunFormat(raw.runFormat);
  if (runFormat) run.runFormat = runFormat;
  if (typeof raw.attachUrl === "string") run.attachUrl = raw.attachUrl;
  if (typeof raw.port === "number" && Number.isFinite(raw.port)) {
    run.port = raw.port;
  }
  return run;
}
function normalizeJob(raw) {
  if (!isRecord(raw)) return null;
  if (typeof raw.slug !== "string" || typeof raw.name !== "string" || typeof raw.schedule !== "string") {
    return null;
  }
  const job = {
    scopeId: typeof raw.scopeId === "string" ? raw.scopeId : void 0,
    slug: raw.slug,
    name: raw.name,
    schedule: raw.schedule,
    source: typeof raw.source === "string" ? raw.source : void 0,
    workdir: typeof raw.workdir === "string" ? raw.workdir : void 0,
    timeoutSeconds: typeof raw.timeoutSeconds === "number" ? raw.timeoutSeconds : void 0,
    worktree: raw.worktree === true ? true : void 0,
    worktreeBase: typeof raw.worktreeBase === "string" ? raw.worktreeBase : void 0,
    createdAt: typeof raw.createdAt === "string" ? raw.createdAt : (/* @__PURE__ */ new Date()).toISOString(),
    updatedAt: typeof raw.updatedAt === "string" ? raw.updatedAt : void 0,
    lastRunAt: typeof raw.lastRunAt === "string" ? raw.lastRunAt : void 0,
    lastRunExitCode: typeof raw.lastRunExitCode === "number" ? raw.lastRunExitCode : void 0,
    lastRunError: typeof raw.lastRunError === "string" ? raw.lastRunError : void 0,
    lastRunSource: raw.lastRunSource === "manual" || raw.lastRunSource === "scheduled" ? raw.lastRunSource : void 0,
    lastRunStatus: raw.lastRunStatus === "running" || raw.lastRunStatus === "success" || raw.lastRunStatus === "failed" ? raw.lastRunStatus : void 0
  };
  if (typeof raw.prompt === "string") job.prompt = raw.prompt;
  if (typeof raw.attachUrl === "string") job.attachUrl = raw.attachUrl;
  const run = normalizeJobRun(raw.run);
  if (run) job.run = run;
  const inv = normalizeJobInvocation(raw.invocation);
  if (inv) job.invocation = inv;
  return sanitizeJob(job);
}
function findJobByName(name, options) {
  const scopeId = options?.scopeId ?? currentScopeId();
  const slug = slugify(name);
  let job = loadScopedJob(scopeId, slug) || loadScopedJob(scopeId, name);
  if (!job) {
    const allJobs = loadAllScopedJobs(scopeId);
    job = allJobs.find(
      (j) => j.slug === name || j.slug.endsWith(`-${slug}`) || j.name.toLowerCase() === name.toLowerCase() || j.name.toLowerCase().includes(name.toLowerCase())
    ) || null;
  }
  if (!job && options?.allScopes) {
    const allJobs = loadAllJobsAcrossScopes();
    job = allJobs.find(
      (j) => j.slug === name || j.slug.endsWith(`-${slug}`) || j.name.toLowerCase() === name.toLowerCase() || j.name.toLowerCase().includes(name.toLowerCase())
    ) || null;
  }
  if (!job && options?.includeLegacy) {
    job = loadLegacyJob(slug) || loadLegacyJob(name);
    if (!job) {
      const allJobs = loadAllLegacyJobs();
      job = allJobs.find(
        (j) => j.slug === name || j.slug.endsWith(`-${slug}`) || j.name.toLowerCase() === name.toLowerCase() || j.name.toLowerCase().includes(name.toLowerCase())
      ) || null;
    }
  }
  return job;
}
function updateJobRecord(job, updates) {
  const scopeId = job.scopeId || deriveScopeId(job.workdir || homedir());
  const latest = loadScopedJob(scopeId, job.slug) || job;
  const updated = {
    ...latest,
    ...updates,
    scopeId,
    updatedAt: (/* @__PURE__ */ new Date()).toISOString()
  };
  saveJob(updated);
  return updated;
}
function getLogPath(job) {
  const scopeId = job.scopeId || deriveScopeId(job.workdir || homedir());
  return scopedLogPath(scopeId, job.slug);
}
function buildOpencodeArgs(job) {
  const command = findOpencode();
  const run = normalizeRunSpec(getJobRun(job));
  validateRunSpec(run);
  const args = ["run"];
  if (run.attachUrl) {
    args.push("--attach", run.attachUrl);
  }
  if (run.port !== void 0) {
    args.push("--port", String(run.port));
  }
  if (run.command) {
    args.push("--command", run.command);
  }
  if (run.agent) {
    args.push("--agent", run.agent);
  }
  if (run.model) {
    args.push("--model", run.model);
  }
  if (run.variant) {
    args.push("--variant", run.variant);
  }
  if (run.runFormat) {
    args.push("--format", run.runFormat);
  }
  if (run.share) {
    args.push("--share");
  }
  if (run.title) {
    args.push("--title", run.title);
  }
  if (run.continue) {
    args.push("--continue");
  }
  if (run.session) {
    args.push("--session", run.session);
  }
  for (const file of run.files ?? []) {
    args.push("--file", file);
  }
  args.push("--");
  args.push(run.command ? run.arguments ?? "" : run.prompt ?? "");
  if (job.worktree) {
    ensureWorktreeWrapper();
    const scopeId = job.scopeId || deriveScopeId(job.workdir || homedir());
    const base = job.worktreeBase && job.worktreeBase.trim() ? job.worktreeBase.trim() : "HEAD";
    return {
      command: "/bin/bash",
      args: [WORKTREE_WRAPPER_PATH, scopeId, job.slug, base, worktreeBaseDir(scopeId), "--", command, ...args]
    };
  }
  return { command, args };
}
function buildRunEnvironment() {
  const enhancedPath = getEnhancedPath();
  const existingPath = process.env.PATH;
  const combinedPath = existingPath ? `${enhancedPath}:${existingPath}` : enhancedPath;
  const basePolicy = { question: "deny" };
  const mergedPolicy = (() => {
    const raw = process.env.OPENCODE_PERMISSION;
    if (!raw) return basePolicy;
    try {
      const existing = JSON.parse(raw);
      if (isRecord(existing)) {
        return { ...existing, ...basePolicy };
      }
    } catch {
    }
    return basePolicy;
  })();
  const baseEnv = { ...process.env };
  const config = loadSchedulerConfig();
  const preserveOpencodeEnv = config.env?.preserveOpencodeEnv === true;
  const preserved = /* @__PURE__ */ new Set(["OPENCODE_PERMISSION", ...config.env?.preserve ?? []]);
  if (!preserveOpencodeEnv) {
    for (const key of Object.keys(baseEnv)) {
      if (!key.startsWith("OPENCODE_")) continue;
      if (key.startsWith("OPENCODE_SCHEDULER_")) continue;
      if (preserved.has(key)) continue;
      delete baseEnv[key];
    }
  }
  return {
    ...baseEnv,
    ...config.env?.set,
    PATH: combinedPath,
    OPENCODE_PERMISSION: JSON.stringify(mergedPolicy)
  };
}
function loadSchedulerConfig() {
  if (!existsSync(SCHEDULER_CONFIG)) return {};
  try {
    const raw = readFileSync(SCHEDULER_CONFIG, "utf-8");
    const parsed = JSON.parse(raw);
    if (!isRecord(parsed)) return {};
    return parsed;
  } catch {
    return {};
  }
}
function getOpencodeVersion(opencodePath) {
  try {
    const output = execSync(`"${opencodePath}" --version`, { env: buildRunEnvironment() }).toString().trim();
    return output || null;
  } catch {
    return null;
  }
}
function runJobNow(job) {
  ensureDir(LOGS_DIR);
  ensureDir(scopeLogsDir(job.scopeId || deriveScopeId(job.workdir || homedir())));
  const startedAt = (/* @__PURE__ */ new Date()).toISOString();
  const logPath = getLogPath(job);
  const logStream = createWriteStream(logPath, { flags: "a" });
  const workdir = job.workdir || homedir();
  logStream.write(`
=== Manual run ${startedAt} ===
`);
  const { command, args } = job.invocation ?? buildOpencodeArgs(job);
  let child;
  try {
    child = spawn(command, args, {
      cwd: workdir,
      env: buildRunEnvironment(),
      stdio: ["ignore", "pipe", "pipe"]
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    logStream.write(`
=== Run error ${(/* @__PURE__ */ new Date()).toISOString()} ===
${message}
`);
    logStream.end();
    updateJobRecord(job, {
      lastRunStatus: "failed",
      lastRunExitCode: void 0,
      lastRunError: message
    });
    throw error;
  }
  const runningJob = updateJobRecord(job, {
    lastRunAt: startedAt,
    lastRunSource: "manual",
    lastRunStatus: "running",
    lastRunExitCode: void 0,
    lastRunError: void 0
  });
  if (child.stdout) child.stdout.pipe(logStream);
  if (child.stderr) child.stderr.pipe(logStream);
  child.on("error", (error) => {
    logStream.write(`
=== Run error ${(/* @__PURE__ */ new Date()).toISOString()} ===
${error.message}
`);
    logStream.end();
    updateJobRecord(job, {
      lastRunStatus: "failed",
      lastRunExitCode: void 0,
      lastRunError: error.message
    });
  });
  child.on("close", (code) => {
    const exitCode = typeof code === "number" ? code : void 0;
    logStream.write(`
=== Run complete (${exitCode ?? "unknown"}) ${(/* @__PURE__ */ new Date()).toISOString()} ===
`);
    logStream.end();
    updateJobRecord(job, {
      lastRunStatus: exitCode === 0 ? "success" : "failed",
      lastRunExitCode: exitCode,
      lastRunError: exitCode === 0 ? void 0 : `Exit code ${exitCode ?? "unknown"}`
    });
  });
  return { startedAt, logPath, pid: child.pid, job: runningJob };
}
function describeCron(cron) {
  const parts = cron.split(" ");
  if (parts.length !== 5) return cron;
  const [min, hour, dom, mon, dow] = parts;
  if (mon === "*" && dom === "*") {
    if (dow === "*" && hour !== "*" && min !== "*" && !hour.includes("*") && !hour.includes("/")) {
      const h = parseInt(hour);
      const m = parseInt(min);
      const ampm = h >= 12 ? "PM" : "AM";
      const displayH = h > 12 ? h - 12 : h === 0 ? 12 : h;
      return `daily at ${displayH}:${m.toString().padStart(2, "0")} ${ampm}`;
    }
    if (hour.startsWith("*/")) {
      return `every ${hour.slice(2)} hours`;
    }
    if (min.startsWith("*/")) {
      return `every ${min.slice(2)} minutes`;
    }
  }
  if (dow !== "*" && dom === "*" && mon === "*") {
    const days = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"];
    const day = days[parseInt(dow)];
    if (day && hour !== "*") {
      const h = parseInt(hour);
      const ampm = h >= 12 ? "PM" : "AM";
      const displayH = h > 12 ? h - 12 : h === 0 ? 12 : h;
      return `${day}s at ${displayH}:${(min || "00").padStart(2, "0")} ${ampm}`;
    }
  }
  return cron;
}
function formatJobDetails(job) {
  const lines = [
    `Job: ${job.name}`,
    `Slug: ${job.slug}`,
    `Schedule: ${job.schedule} (${describeCron(job.schedule)})`,
    `Working Directory: ${job.workdir || homedir()}`
  ];
  const run = (() => {
    try {
      return normalizeRunSpec(getJobRun(job));
    } catch {
      return void 0;
    }
  })();
  if (run?.attachUrl) {
    lines.push(`Attach URL: ${run.attachUrl}`);
  } else if (job.attachUrl) {
    lines.push(`Attach URL: ${job.attachUrl}`);
  }
  if (run?.command) {
    lines.push(`Command: ${run.command}`);
    if (run.arguments) lines.push(`Arguments: ${run.arguments}`);
  }
  if (run?.prompt) {
    lines.push(`Prompt: ${run.prompt}`);
  } else if (job.prompt) {
    lines.push(`Prompt: ${job.prompt}`);
  }
  if (run?.files?.length) {
    lines.push(`Files: ${run.files.join(", ")}`);
  }
  if (run?.agent) {
    lines.push(`Agent: ${run.agent}`);
  }
  if (run?.model) {
    lines.push(`Model: ${run.model}`);
  }
  if (run?.variant) {
    lines.push(`Variant: ${run.variant}`);
  }
  if (run?.runFormat) {
    lines.push(`Run Format: ${run.runFormat}`);
  }
  if (run?.title) {
    lines.push(`Title: ${run.title}`);
  }
  if (run?.share) {
    lines.push("Share: true");
  }
  if (run?.continue) {
    lines.push("Continue: true");
  }
  if (run?.session) {
    lines.push(`Session: ${run.session}`);
  }
  if (run?.port !== void 0) {
    lines.push(`Port: ${run.port}`);
  }
  lines.push(`Created: ${job.createdAt}`);
  if (job.updatedAt) {
    lines.push(`Updated: ${job.updatedAt}`);
  }
  if (job.lastRunAt) {
    lines.push(`Last Run: ${job.lastRunAt}`);
  }
  if (job.lastRunSource) {
    lines.push(`Last Run Source: ${job.lastRunSource}`);
  }
  if (job.lastRunStatus) {
    lines.push(`Last Run Status: ${job.lastRunStatus}`);
  }
  if (job.lastRunExitCode !== void 0) {
    lines.push(`Last Exit Code: ${job.lastRunExitCode}`);
  }
  if (job.lastRunError) {
    lines.push(`Last Error: ${job.lastRunError}`);
  }
  return lines.join("\n");
}
function getJobLogs(job, options) {
  const logPath = getLogPath(job);
  if (!existsSync(logPath)) return null;
  const maxChars = options?.maxChars ?? 5e3;
  const tailLines = options?.tailLines;
  try {
    if (typeof tailLines === "number" && Number.isFinite(tailLines) && tailLines > 0) {
      const clampedLines = Math.max(1, Math.min(5e3, Math.floor(tailLines)));
      try {
        const output = execFileSync("tail", ["-n", String(clampedLines), logPath], {
          env: buildRunEnvironment()
        }).toString();
        return output.length > maxChars ? output.slice(-maxChars) : output;
      } catch {
        const content2 = readFileSync(logPath, "utf-8");
        const lines = content2.split(/\r?\n/);
        const output = lines.slice(-clampedLines).join("\n");
        return output.length > maxChars ? output.slice(-maxChars) : output;
      }
    }
    const content = readFileSync(logPath, "utf-8");
    return content.length > maxChars ? content.slice(-maxChars) : content;
  } catch {
    return null;
  }
}
var SchedulerPlugin = async () => {
  return {
    tool: {
      schedule_job: tool({
        description: "Schedule a recurring job to run an opencode prompt. Uses launchd (Mac), systemd (Linux), Windows Task Scheduler, or cron fallback when needed.",
        args: {
          name: tool.schema.string().describe("A short name for the job (e.g. 'standing desk search')"),
          schedule: tool.schema.string().describe("Cron expression: '0 9 * * *' (daily 9am), '0 */6 * * *' (every 6h), '30 8 * * 1' (Monday 8:30am)"),
          prompt: tool.schema.string().optional().describe("Prompt to run (legacy; prefer run fields)"),
          command: tool.schema.string().optional().describe("Optional: opencode command to run (maps to --command)"),
          arguments: tool.schema.string().optional().describe("Optional: arguments string for command mode"),
          files: tool.schema.string().optional().describe("Optional: comma-separated list of files/dirs to attach (maps to repeated --file)"),
          agent: tool.schema.string().optional().describe("Optional: agent to use (maps to --agent)"),
          model: tool.schema.string().optional().describe("Optional: model to use (maps to --model)"),
          variant: tool.schema.string().optional().describe("Optional: model variant (maps to --variant)"),
          title: tool.schema.string().optional().describe("Optional: session title (maps to --title)"),
          share: tool.schema.boolean().optional().describe("Optional: share session (maps to --share)"),
          continue: tool.schema.boolean().optional().describe("Optional: continue last session (maps to --continue)"),
          session: tool.schema.string().optional().describe("Optional: session id (maps to --session)"),
          runFormat: tool.schema.string().optional().describe("Optional: run output format (maps to opencode --format: default|json)"),
          port: tool.schema.number().optional().describe("Optional: server port for local server (maps to --port)"),
          source: tool.schema.string().optional().describe("Optional: source app (e.g. 'marketplace') - used for filtering"),
          workdir: tool.schema.string().optional().describe("Optional: working directory to run from (for MCP config). Defaults to current directory."),
          attachUrl: tool.schema.string().optional().describe("Optional: attach URL for opencode run (e.g. http://localhost:4096)."),
          timeoutSeconds: tool.schema.number().optional().describe("Optional: max runtime in seconds (0 disables)."),
          worktree: tool.schema.boolean().optional().describe(
            "Optional: run each execution in an isolated git worktree (created per run off worktreeBase, left in place for later review; reap with cleanup_worktrees)."
          ),
          worktreeBase: tool.schema.string().optional().describe("Optional: git ref to base the worktree on (branch/commit). Defaults to HEAD of the repo at workdir."),
          format: tool.schema.string().optional().describe("Optional: output format ('text' or 'json').")
        },
        async execute(args) {
          const format = normalizeFormat(args.format);
          const slug = args.source ? `${args.source}-${slugify(args.name)}` : slugify(args.name);
          const workdir = normalizeWorkdirPath(args.workdir || process.cwd());
          const scopeId = deriveScopeId(workdir);
          if (loadScopedJob(scopeId, slug)) {
            return errorResult(
              format,
              `Job "${slug}" already exists in this workspace scope (${scopeId}). Delete it first or use a different name.`
            );
          }
          if (loadLegacyJob(slug)) {
            return errorResult(
              format,
              `Job "${slug}" already exists (legacy scheduler storage). Delete it first or use a different name.`
            );
          }
          const parseFiles = (raw) => {
            if (raw === void 0) return void 0;
            if (typeof raw !== "string") return void 0;
            const items = raw.split(",").map((item) => item.trim()).filter(Boolean);
            return items.length ? items : void 0;
          };
          let runFormat;
          try {
            runFormat = parseRunFormatInput(args.runFormat);
          } catch (error) {
            const msg = error instanceof Error ? error.message : String(error);
            return errorResult(format, msg);
          }
          const run = {
            prompt: args.prompt,
            command: args.command,
            arguments: args.arguments,
            files: parseFiles(args.files),
            agent: args.agent,
            model: args.model,
            variant: args.variant,
            title: args.title,
            share: args.share,
            continue: args.continue,
            session: args.session,
            runFormat,
            attachUrl: args.attachUrl,
            port: args.port
          };
          try {
            validateRunSpec(normalizeRunSpec(run));
          } catch (error) {
            const msg = error instanceof Error ? error.message : String(error);
            return errorResult(format, `Invalid run spec: ${msg}`);
          }
          let attachUrl;
          try {
            attachUrl = normalizeAttachUrl(args.attachUrl);
          } catch (error) {
            const msg = error instanceof Error ? error.message : String(error);
            return errorResult(format, msg);
          }
          try {
            validateCronExpression(args.schedule);
          } catch (error) {
            const msg = error instanceof Error ? error.message : String(error);
            return errorResult(format, `Invalid cron schedule: ${msg}`);
          }
          const job = {
            scopeId,
            slug,
            name: args.name,
            schedule: args.schedule,
            run: normalizeRunSpec(run),
            // keep legacy fields as well for backwards-compat / readability
            prompt: args.prompt,
            source: args.source,
            workdir,
            attachUrl,
            timeoutSeconds: args.timeoutSeconds,
            worktree: args.worktree,
            worktreeBase: args.worktreeBase,
            createdAt: (/* @__PURE__ */ new Date()).toISOString()
          };
          try {
            job.invocation = buildOpencodeArgs(job);
          } catch (error) {
            const msg = error instanceof Error ? error.message : String(error);
            return errorResult(format, `Failed to build invocation: ${msg}`);
          }
          try {
            saveJob(job);
            const backend = installJob(job);
            const platformName = backend;
            const reliabilityLine = backend === "schtasks" ? "Windows note: scheduled runs use Task Scheduler directly. For advanced reliability guarantees, prefer simple cron schedules or split complex jobs." : backend === "cron" ? "Cron note: missed runs during sleep are not replayed. For catch-up behavior, use launchd or systemd when available." : "The job will run at the scheduled time. If your computer was asleep, it will catch up when it wakes.";
            const primaryLine = run.command ? `Command: ${run.command}${run.arguments ? ` ${run.arguments}` : ""}` : `Prompt: ${(run.prompt ?? "").slice(0, 100)}${(run.prompt ?? "").length > 100 ? "..." : ""}`;
            const attachLine = run.attachUrl ? `Attach URL: ${run.attachUrl}
` : "";
            return okResult(
              format,
              `Scheduled "${args.name}"

Schedule: ${args.schedule} (${describeCron(args.schedule)})
Platform: ${platformName}
Working Directory: ${workdir}
${attachLine}${primaryLine}

${reliabilityLine}

Commands:
- "run ${args.name} now" - run immediately
- "show my jobs" - list all
- "delete job ${args.name}" - remove`,
              { job }
            );
          } catch (error) {
            deleteJobFile(job);
            const msg = error instanceof Error ? error.message : String(error);
            return errorResult(format, `Failed to schedule job: ${msg}`);
          }
        }
      }),
      list_jobs: tool({
        description: "List all scheduled jobs. Optionally filter by source app.",
        args: {
          source: tool.schema.string().optional().describe("Filter by source app (e.g. 'marketplace')"),
          allScopes: tool.schema.boolean().optional().describe("List jobs across all scopes."),
          includeLegacy: tool.schema.boolean().optional().describe("Include legacy jobs from ~/.config/opencode/jobs"),
          scopeRoot: tool.schema.string().optional().describe("Optional: scope root directory (defaults to current directory)."),
          format: tool.schema.string().optional().describe("Optional: output format ('text' or 'json').")
        },
        async execute(args) {
          const format = normalizeFormat(args.format);
          const scopeId = args.allScopes ? void 0 : deriveScopeId(normalizeWorkdirPath(args.scopeRoot || process.cwd()));
          let jobs = args.allScopes ? loadAllJobsAcrossScopes() : loadAllScopedJobs(scopeId);
          if (args.includeLegacy) {
            jobs = [...jobs, ...loadAllLegacyJobs()];
          }
          if (args.source) {
            jobs = jobs.filter((j) => j.source === args.source || j.slug.startsWith(`${args.source}-`));
          }
          if (jobs.length === 0) {
            const message = args.source ? `No jobs found for "${args.source}".` : 'No scheduled jobs yet.\n\nTry: "Schedule a daily job at 9am to search for standing desks"';
            return okResult(format, message, { jobs: [] });
          }
          const lines = jobs.map((j, i) => {
            const run = (() => {
              try {
                return normalizeRunSpec(getJobRun(j));
              } catch {
                return void 0;
              }
            })();
            const preview = run?.command ? `${run.command}${run.arguments ? ` ${run.arguments}` : ""}` : run?.prompt ?? j.prompt ?? "(missing prompt)";
            const trimmed = preview.trim();
            const snippet = trimmed.slice(0, 50) + (trimmed.length > 50 ? "..." : "");
            return `${i + 1}. ${j.name} (${j.slug})
   ${describeCron(j.schedule)}
   ${snippet}`;
          });
          return okResult(format, `Scheduled Jobs

${lines.join("\n\n")}`, { jobs });
        }
      }),
      get_version: tool({
        description: "Show the scheduler plugin version and opencode binary info.",
        args: {
          format: tool.schema.string().optional().describe("Optional: output format ('text' or 'json').")
        },
        async execute(args) {
          const format = normalizeFormat(args.format);
          const packageInfo = loadPackageInfo();
          const opencodePath = findOpencode();
          const opencodeVersion = getOpencodeVersion(opencodePath);
          const lines = [
            `Scheduler Plugin: ${packageInfo.name}@${packageInfo.version}`,
            `Opencode Binary: ${opencodePath}`,
            `Opencode Version: ${opencodeVersion ?? "unknown"}`
          ];
          return okResult(format, lines.join("\n"), {
            plugin: packageInfo,
            opencode: { path: opencodePath, version: opencodeVersion },
            platform: platform()
          });
        }
      }),
      get_skill: tool({
        description: "Get built-in skill templates to copy into your project.",
        args: {
          name: tool.schema.string().optional().describe("Skill name (default: scheduled-job-best-practices)"),
          format: tool.schema.string().optional().describe("Optional: output format ('text' or 'json').")
        },
        async execute(args) {
          const format = normalizeFormat(args.format);
          const skill = getBuiltinSkill(args.name);
          if (!skill) {
            const available = listBuiltinSkills().map((s) => s.name).join(", ");
            const requested = (args.name ?? "").trim();
            const label = requested ? `"${requested}"` : "that name";
            return errorResult(format, `No built-in skill found for ${label}. Available: ${available || "(none)"}`);
          }
          const renderedFiles = Object.entries(skill.files).map(([filename, content]) => `--- ${filename} ---
${content.trim()}
`).join("\n");
          const output = [
            `Skill: ${skill.name}`,
            `Description: ${skill.description}`,
            `Suggested path: ${skill.suggestedPath}`,
            "",
            "Copy the file(s) below into your repo:",
            "",
            renderedFiles
          ].join("\n");
          return okResult(format, output, { skill });
        }
      }),
      install_skill: tool({
        description: "Install a built-in skill into your repo's .opencode/skill directory.",
        args: {
          name: tool.schema.string().optional().describe("Skill name (default: scheduled-job-best-practices)"),
          directory: tool.schema.string().optional().describe("Repo root directory to install into (defaults to current directory)."),
          overwrite: tool.schema.boolean().optional().describe("Overwrite existing files (default false)."),
          format: tool.schema.string().optional().describe("Optional: output format ('text' or 'json').")
        },
        async execute(args) {
          const format = normalizeFormat(args.format);
          const skill = getBuiltinSkill(args.name);
          if (!skill) {
            const available = listBuiltinSkills().map((s) => s.name).join(", ");
            const requested = (args.name ?? "").trim();
            const label = requested ? `"${requested}"` : "that name";
            return errorResult(format, `No built-in skill found for ${label}. Available: ${available || "(none)"}`);
          }
          const directory = args.directory ?? process.cwd();
          const overwrite = args.overwrite === true;
          try {
            const installed = installBuiltinSkill(skill, directory, overwrite);
            const files = installed.files.map((file) => `- ${file}`).join("\n");
            const output = [
              `Installed skill: ${skill.name}`,
              `Directory: ${installed.directory}`,
              "",
              "Files:",
              files,
              "",
              `Next: add @${skill.name} to the top of scheduled job prompts.`
            ].join("\n");
            return okResult(format, output, { skill, installed });
          } catch (error) {
            const msg = error instanceof Error ? error.message : String(error);
            return errorResult(format, `Failed to install skill: ${msg}`);
          }
        }
      }),
      get_job: tool({
        description: "Get details for a scheduled job",
        args: {
          name: tool.schema.string().describe("The job name or slug"),
          format: tool.schema.string().optional().describe("Optional: output format ('text' or 'json').")
        },
        async execute(args) {
          const format = normalizeFormat(args.format);
          const job = findJobByName(args.name);
          if (!job) {
            return errorResult(format, `Job "${args.name}" not found.`);
          }
          return okResult(format, formatJobDetails(job), { job });
        }
      }),
      update_job: tool({
        description: "Update a scheduled job",
        args: {
          name: tool.schema.string().describe("The job name or slug"),
          schedule: tool.schema.string().optional().describe("Updated cron expression"),
          // Legacy prompt field
          prompt: tool.schema.string().optional().describe("Updated prompt (legacy; prefer command/arguments/etc)"),
          command: tool.schema.string().optional().describe("Updated opencode command (maps to --command)"),
          arguments: tool.schema.string().optional().describe("Updated command arguments string"),
          files: tool.schema.string().optional().describe("Updated comma-separated list of files/dirs to attach"),
          agent: tool.schema.string().optional().describe("Updated agent (maps to --agent)"),
          model: tool.schema.string().optional().describe("Updated model (maps to --model)"),
          variant: tool.schema.string().optional().describe("Updated model variant (maps to --variant)"),
          title: tool.schema.string().optional().describe("Updated session title (maps to --title)"),
          share: tool.schema.boolean().optional().describe("Updated share flag (maps to --share)"),
          continue: tool.schema.boolean().optional().describe("Updated continue flag (maps to --continue)"),
          session: tool.schema.string().optional().describe("Updated session id (maps to --session)"),
          runFormat: tool.schema.string().optional().describe("Updated run output format (default|json)"),
          port: tool.schema.number().optional().describe("Updated port (maps to --port)"),
          timeoutSeconds: tool.schema.number().optional().describe("Updated timeout in seconds (0 disables)"),
          worktree: tool.schema.boolean().optional().describe("Updated worktree isolation flag (true to isolate each run in a fresh git worktree)"),
          worktreeBase: tool.schema.string().optional().describe("Updated git ref to base the worktree on (defaults to HEAD)"),
          workdir: tool.schema.string().optional().describe("Updated working directory"),
          attachUrl: tool.schema.string().optional().describe("Updated attach URL (set to empty to clear)"),
          format: tool.schema.string().optional().describe("Optional: output format ('text' or 'json').")
        },
        async execute(args) {
          const format = normalizeFormat(args.format);
          const job = findJobByName(args.name);
          if (!job) {
            return errorResult(format, `Job "${args.name}" not found.`);
          }
          const updates = {};
          const parseFiles = (raw) => {
            if (raw === void 0) return void 0;
            if (typeof raw !== "string") return void 0;
            const items = raw.split(",").map((item) => item.trim()).filter(Boolean);
            return items.length ? items : void 0;
          };
          const currentRun = (() => {
            try {
              return normalizeRunSpec(getJobRun(job));
            } catch {
              return {};
            }
          })();
          const nextRunCandidate = {
            ...currentRun,
            prompt: args.prompt !== void 0 ? args.prompt : currentRun.prompt,
            command: args.command !== void 0 ? args.command : currentRun.command,
            arguments: args.arguments !== void 0 ? args.arguments : currentRun.arguments,
            files: args.files !== void 0 ? parseFiles(args.files) : currentRun.files,
            agent: args.agent !== void 0 ? args.agent : currentRun.agent,
            model: args.model !== void 0 ? args.model : currentRun.model,
            variant: args.variant !== void 0 ? args.variant : currentRun.variant,
            title: args.title !== void 0 ? args.title : currentRun.title,
            share: args.share !== void 0 ? args.share : currentRun.share,
            continue: args.continue !== void 0 ? args.continue : currentRun.continue,
            session: args.session !== void 0 ? args.session : currentRun.session,
            runFormat: args.runFormat !== void 0 ? parseRunFormatInput(args.runFormat) : currentRun.runFormat,
            attachUrl: args.attachUrl !== void 0 ? args.attachUrl : currentRun.attachUrl,
            port: args.port !== void 0 ? args.port : currentRun.port
          };
          try {
            updates.run = normalizeRunSpec(nextRunCandidate);
          } catch (error) {
            const msg = error instanceof Error ? error.message : String(error);
            return errorResult(format, `Invalid run spec: ${msg}`);
          }
          if (args.schedule !== void 0) {
            if (!args.schedule.trim()) {
              return errorResult(format, "Schedule cannot be empty.");
            }
            try {
              validateCronExpression(args.schedule);
            } catch (error) {
              const msg = error instanceof Error ? error.message : String(error);
              return errorResult(format, `Invalid cron schedule: ${msg}`);
            }
            updates.schedule = args.schedule;
          }
          if (args.prompt !== void 0) {
            if (!args.prompt.trim()) {
              return errorResult(format, "Prompt cannot be empty.");
            }
            updates.prompt = args.prompt;
          }
          if (args.workdir !== void 0) {
            if (!args.workdir.trim()) {
              return errorResult(format, "Working directory cannot be empty.");
            }
            const normalizedWorkdir = normalizeWorkdirPath(args.workdir);
            updates.workdir = normalizedWorkdir;
            updates.scopeId = deriveScopeId(normalizedWorkdir);
          }
          if (args.attachUrl !== void 0) {
            try {
              updates.attachUrl = normalizeAttachUrl(args.attachUrl);
            } catch (error) {
              const msg = error instanceof Error ? error.message : String(error);
              return errorResult(format, msg);
            }
          }
          if (args.timeoutSeconds !== void 0) {
            updates.timeoutSeconds = args.timeoutSeconds;
          }
          if (args.worktree !== void 0) {
            updates.worktree = args.worktree;
          }
          if (args.worktreeBase !== void 0) {
            updates.worktreeBase = args.worktreeBase;
          }
          if (Object.keys(updates).length === 0) {
            return errorResult(format, "No updates provided.");
          }
          const updatedJob = {
            ...job,
            ...updates,
            updatedAt: (/* @__PURE__ */ new Date()).toISOString()
          };
          try {
            updatedJob.invocation = buildOpencodeArgs(updatedJob);
          } catch (error) {
            const msg = error instanceof Error ? error.message : String(error);
            return errorResult(format, `Failed to build invocation: ${msg}`);
          }
          try {
            const oldScopeId = job.scopeId || deriveScopeId(job.workdir || homedir());
            const nextScopeId = updatedJob.scopeId || deriveScopeId(updatedJob.workdir || homedir());
            const scopeChanged = oldScopeId !== nextScopeId;
            if (scopeChanged) {
              uninstallJob(job);
            }
            saveJob(updatedJob);
            installJob(updatedJob);
            if (scopeChanged) {
              const oldPath = jobFilePath(oldScopeId, job.slug);
              if (existsSync(oldPath)) {
                try {
                  unlinkSync(oldPath);
                } catch {
                }
              }
            }
            return okResult(format, `Updated job "${updatedJob.name}"`, { job: updatedJob });
          } catch (error) {
            const msg = error instanceof Error ? error.message : String(error);
            try {
              saveJob(job);
              installJob(job);
            } catch {
            }
            return errorResult(format, `Failed to update job: ${msg}`);
          }
        }
      }),
      delete_job: tool({
        description: "Delete a scheduled job",
        args: {
          name: tool.schema.string().describe("The job name or slug to delete"),
          format: tool.schema.string().optional().describe("Optional: output format ('text' or 'json').")
        },
        async execute(args) {
          const format = normalizeFormat(args.format);
          const job = findJobByName(args.name);
          if (!job) {
            return errorResult(format, `Job "${args.name}" not found.`);
          }
          uninstallJob(job);
          deleteJobFile(job);
          const legacyPath = join(LEGACY_JOBS_DIR, `${job.slug}.json`);
          if (existsSync(legacyPath)) {
            try {
              unlinkSync(legacyPath);
            } catch {
            }
          }
          return okResult(format, `Deleted job "${job.name}"`, { job });
        }
      }),
      cleanup_global: tool({
        description: "Clean up scheduler artifacts globally across all scopes. Removes job definitions everywhere; optionally remove logs and run history.",
        args: {
          includeHistory: tool.schema.boolean().optional().describe("Also remove run history and logs across all scopes (default false)."),
          confirm: tool.schema.boolean().optional().describe("Set true to execute deletion. Default is dry run with no destructive changes."),
          format: tool.schema.string().optional().describe("Optional: output format ('text' or 'json').")
        },
        async execute(args) {
          const format = normalizeFormat(args.format);
          const includeHistory = args.includeHistory === true;
          const dryRun = args.confirm !== true;
          const plan = buildGlobalCleanupPlan(includeHistory);
          const execution = executeGlobalCleanup(plan, { dryRun, includeHistory });
          const output = formatGlobalCleanupOutput(execution);
          return okResult(format, output, {
            dryRun: execution.dryRun,
            includeHistory: execution.includeHistory,
            removed: execution.removed,
            errors: execution.errors,
            scopeIds: plan.scopeIds,
            jobsConsidered: plan.jobsToUninstall.length
          });
        }
      }),
      run_job: tool({
        description: "Run a scheduled job immediately",
        args: {
          name: tool.schema.string().describe("The job name or slug"),
          // Optional overrides for a one-off run
          prompt: tool.schema.string().optional().describe("Override prompt for this run"),
          command: tool.schema.string().optional().describe("Override command for this run"),
          arguments: tool.schema.string().optional().describe("Override arguments for command mode"),
          files: tool.schema.string().optional().describe("Override comma-separated files/dirs to attach"),
          agent: tool.schema.string().optional().describe("Override agent"),
          model: tool.schema.string().optional().describe("Override model"),
          variant: tool.schema.string().optional().describe("Override variant"),
          title: tool.schema.string().optional().describe("Override title"),
          share: tool.schema.boolean().optional().describe("Override share flag"),
          continue: tool.schema.boolean().optional().describe("Override continue flag"),
          session: tool.schema.string().optional().describe("Override session id"),
          runFormat: tool.schema.string().optional().describe("Override run output format (default|json)"),
          port: tool.schema.number().optional().describe("Override port"),
          attachUrl: tool.schema.string().optional().describe("Override attach URL"),
          format: tool.schema.string().optional().describe("Optional: output format ('text' or 'json').")
        },
        async execute(args) {
          const format = normalizeFormat(args.format);
          const job = findJobByName(args.name);
          if (!job) {
            return errorResult(format, `Job "${args.name}" not found. Use list_jobs to see available jobs.`);
          }
          const parseFiles = (raw) => {
            if (raw === void 0) return void 0;
            if (typeof raw !== "string") return void 0;
            const items = raw.split(",").map((item) => item.trim()).filter(Boolean);
            return items.length ? items : void 0;
          };
          const baseRun = (() => {
            try {
              return normalizeRunSpec(getJobRun(job));
            } catch {
              return {};
            }
          })();
          const hasOverride = args.prompt !== void 0 || args.command !== void 0 || args.arguments !== void 0 || args.files !== void 0 || args.agent !== void 0 || args.model !== void 0 || args.variant !== void 0 || args.title !== void 0 || args.share !== void 0 || args.continue !== void 0 || args.session !== void 0 || args.runFormat !== void 0 || args.port !== void 0 || args.attachUrl !== void 0;
          const overrideCandidate = {
            ...baseRun,
            prompt: args.prompt !== void 0 ? args.prompt : baseRun.prompt,
            command: args.command !== void 0 ? args.command : baseRun.command,
            arguments: args.arguments !== void 0 ? args.arguments : baseRun.arguments,
            files: args.files !== void 0 ? parseFiles(args.files) : baseRun.files,
            agent: args.agent !== void 0 ? args.agent : baseRun.agent,
            model: args.model !== void 0 ? args.model : baseRun.model,
            variant: args.variant !== void 0 ? args.variant : baseRun.variant,
            title: args.title !== void 0 ? args.title : baseRun.title,
            share: args.share !== void 0 ? args.share : baseRun.share,
            continue: args.continue !== void 0 ? args.continue : baseRun.continue,
            session: args.session !== void 0 ? args.session : baseRun.session,
            runFormat: args.runFormat !== void 0 ? parseRunFormatInput(args.runFormat) : baseRun.runFormat,
            port: args.port !== void 0 ? args.port : baseRun.port,
            attachUrl: args.attachUrl !== void 0 ? args.attachUrl : baseRun.attachUrl
          };
          let runOverride;
          try {
            runOverride = normalizeRunSpec(overrideCandidate);
            validateRunSpec(runOverride);
          } catch (error) {
            const msg = error instanceof Error ? error.message : String(error);
            return errorResult(format, `Invalid run override: ${msg}`);
          }
          const runJob = {
            ...job,
            run: runOverride
          };
          if (hasOverride) {
            try {
              runJob.invocation = buildOpencodeArgs(runJob);
            } catch (error) {
              const msg = error instanceof Error ? error.message : String(error);
              return errorResult(format, `Failed to build invocation: ${msg}`);
            }
          }
          let runResult;
          try {
            runResult = runJobNow(runJob);
          } catch (error) {
            const msg = error instanceof Error ? error.message : String(error);
            return errorResult(format, `Failed to start job "${job.name}": ${msg}`);
          }
          const logs = getJobLogs(runJob);
          const attachHint = runOverride.attachUrl ? `
Attach: opencode attach ${runOverride.attachUrl}` : "";
          const logSection = logs ? `
Latest logs:
${logs}` : "\nNo logs yet. Check again soon.";
          return okResult(
            format,
            `Triggered "${job.name}" (fire-and-forget).
Logs: ${runResult.logPath}${attachHint}${logSection}`,
            {
              job: runResult.job ?? job,
              startedAt: runResult.startedAt,
              logPath: runResult.logPath,
              pid: runResult.pid
            }
          );
        }
      }),
      job_logs: tool({
        description: "View the latest logs from a scheduled job",
        args: {
          name: tool.schema.string().describe("The job name or slug"),
          lines: tool.schema.number().optional().describe("Number of lines from the end of the log (default 200)."),
          format: tool.schema.string().optional().describe("Optional: output format ('text' or 'json').")
        },
        async execute(args) {
          const format = normalizeFormat(args.format);
          const job = findJobByName(args.name);
          if (!job) {
            return errorResult(format, `Job "${args.name}" not found.`);
          }
          const tailLines = typeof args.lines === "number" && Number.isFinite(args.lines) ? args.lines : 200;
          const logs = getJobLogs(job, { tailLines, maxChars: 2e4 });
          const logPath = getLogPath(job);
          if (!logs) {
            return okResult(format, `No logs found for "${job.name}". The job may not have run yet.`, {
              job,
              logPath,
              logs: ""
            });
          }
          return okResult(format, `Logs for ${job.name}

${logs}`, { job, logPath, logs });
        }
      }),
      list_worktrees: tool({
        description: "List isolated git worktrees created by scheduled runs (fork addition). Shows branch, path, age, and whether the worktree still exists on disk.",
        args: {
          slug: tool.schema.string().optional().describe("Optional: only worktrees for this job slug."),
          format: tool.schema.string().optional().describe("Optional: output format ('text' or 'json').")
        },
        async execute(args) {
          const format = normalizeFormat(args.format);
          const metas = listWorktreeMetas({ slug: args.slug });
          if (metas.length === 0) {
            return okResult(format, "No scheduler worktrees found.", { worktrees: [] });
          }
          const lines = metas.map((m) => {
            const age = Number.isFinite(m.ageHours) ? `${m.ageHours.toFixed(1)}h` : "unknown";
            const state = m.exists ? "" : " (missing on disk)";
            return `- ${m.slug} [${m.branch}] age=${age}${state}
    ${m.worktree}`;
          });
          return okResult(format, `Scheduler worktrees (${metas.length}):
${lines.join("\n")}`, {
            worktrees: metas
          });
        }
      }),
      cleanup_worktrees: tool({
        description: "Reap isolated scheduler worktrees older than a threshold (fork addition). Removes the worktree, prunes it from git, and (by default) deletes its scheduler branch. Dry run unless confirm=true.",
        args: {
          olderThanHours: tool.schema.number().optional().describe(`Only reap worktrees created more than this many hours ago (default ${DEFAULT_WORKTREE_KEEP_HOURS}).`),
          slug: tool.schema.string().optional().describe("Optional: only reap worktrees for this job slug."),
          deleteBranch: tool.schema.boolean().optional().describe("Also delete the per-run scheduler branch (default true)."),
          confirm: tool.schema.boolean().optional().describe("Set true to execute removal. Default is a dry run listing what would be removed."),
          format: tool.schema.string().optional().describe("Optional: output format ('text' or 'json').")
        },
        async execute(args) {
          const format = normalizeFormat(args.format);
          const olderThanHours = typeof args.olderThanHours === "number" && Number.isFinite(args.olderThanHours) ? args.olderThanHours : DEFAULT_WORKTREE_KEEP_HOURS;
          const deleteBranch = args.deleteBranch !== false;
          const dryRun = args.confirm !== true;
          const candidates = listWorktreeMetas({ slug: args.slug }).filter((m) => m.ageHours >= olderThanHours);
          if (candidates.length === 0) {
            return okResult(format, `No worktrees older than ${olderThanHours}h to reap.`, {
              dryRun,
              reaped: []
            });
          }
          const reaped = [];
          for (const m of candidates) {
            if (dryRun) {
              reaped.push({ slug: m.slug, branch: m.branch, worktree: m.worktree, ok: true });
              continue;
            }
            const result = removeWorktree(m, deleteBranch);
            reaped.push({ slug: m.slug, branch: m.branch, worktree: m.worktree, ok: result.ok, error: result.error });
          }
          const verb = dryRun ? "Would reap" : "Reaped";
          const lines = reaped.map((r) => {
            const status = r.ok ? "" : ` \u2014 FAILED: ${r.error}`;
            return `- ${r.slug} [${r.branch}]${status}
    ${r.worktree}`;
          });
          const footer = dryRun ? "\n\nDry run \u2014 set confirm=true to remove." : "";
          return okResult(
            format,
            `${verb} ${reaped.length} worktree(s) older than ${olderThanHours}h:
${lines.join("\n")}${footer}`,
            { dryRun, olderThanHours, deleteBranch, reaped }
          );
        }
      })
    }
  };
};
var src_default = SchedulerPlugin;
export {
  SchedulerPlugin,
  src_default as default
};
