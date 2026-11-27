// Tests block-river-source.js against tool calls it must deny and ones it must allow: node .claude/hooks/block-river-source.test.js
'use strict';
const { spawnSync } = require('child_process');
const hook = process.argv[2] ?? require('path').join(__dirname, 'block-river-source.js');
const B = String.fromCharCode(92); // a backslash, built in code so no shell can mangle it
const win = (...parts) => parts.join(B);

const cases = [
  ['deny', 'WebFetch', { url: 'https://github.com/riverqueue/river/blob/master/job_executor.go' }],
  ['deny', 'WebFetch', { url: 'https://github.com/riverqueue/river/tree/master/riverdriver' }],
  ['deny', 'WebFetch', { url: 'https://raw.githubusercontent.com/riverqueue/river/master/x.sql' }],
  ['deny', 'WebFetch', { url: 'https://github.com/riverqueue/river/pull/900/files' }],
  ['deny', 'Bash', { command: 'gh api repos/riverqueue/river/contents/riverdriver' }],
  ['deny', 'Bash', { command: 'git clone https://github.com/riverqueue/river' }],
  ['deny', 'Bash', { command: 'gh repo clone riverqueue/river' }],
  ['deny', 'Read', { file_path: win('C:', 'Users', 'dev', 'go', 'pkg', 'mod', 'github.com', 'riverqueue', 'river@v0.51.2', 'job_executor.go') }],
  ['deny', 'PowerShell', { command: 'Expand-Archive ' + win('C:', 'Users', 'dev', 'go', 'pkg', 'mod', 'cache', 'download', 'github.com', 'riverqueue', 'river', '@v', 'v0.51.2.zip') }],
  ['deny', 'Grep', { pattern: 'JobSchedule', path: '/c/Users/dev/go/pkg/mod/github.com/riverqueue' }],
  ['deny', 'Glob', { pattern: '**/*.sql', path: '~/go/pkg/mod/github.com/riverqueue/river@v0.51.2' }],
  ['deny', 'Bash', { command: 'go doc -src github.com/riverqueue/river.Client.JobCancel' }],
  ['deny', 'Bash', { command: 'go install github.com/riverqueue/river/cmd/river@v0.51.2' }],
  ['deny', 'Bash', { command: 'go mod download github.com/riverqueue/river' }],
  ['deny', 'mcp__claude-in-chrome__navigate', { url: 'https://github.com/riverqueue/river/blob/master/rivershared/x.go' }],
  ['deny', 'Bash', { command: 'cd /tmp\ngit clone https://github.com/riverqueue/river\nls' }],
  ['deny', 'Bash', { command: 'cd x && gh api repos/riverqueue/river/contents' }],
  ['deny', 'Write', { file_path: '/c/Users/dev/go/pkg/mod/github.com/riverqueue/river@v0.51.2/x.go', content: 'x' }],
  ['deny', 'Agent', { prompt: 'Read https://github.com/riverqueue/river/blob/master/job_executor.go and summarise it' }],
  ['allow', 'Write', { file_path: 'CLAUDE.md', content: 'not in a clone\nsee https://riverqueue.com/docs and go doc -src github.com/riverqueue/river' }],
  ['allow', 'Edit', { file_path: 'docs/x.md', old_string: 'github.com/riverqueue/river/blob/x', new_string: 'y' }],
  ['allow', 'Bash', { command: 'git clone https://github.com/sujanto-gaws/KopiQueue && echo riverqueue' }],
  ['allow', 'Bash', { command: 'git commit -m "clone the queue cleaner\n\nsee riverqueue.com/docs"' }],
  ['allow', 'Bash', { command: 'git commit -m "clone the queue cleaner, see riverqueue.com/docs"' }],
  ['deny', 'Bash', { command: 'git clone --depth 1 git@github.com:riverqueue/river.git' }],
  ['allow', 'WebFetch', { url: 'https://riverqueue.com/docs/maintenance-services' }],
  ['allow', 'WebFetch', { url: 'https://github.com/riverqueue/river' }],
  ['allow', 'Read', { file_path: win('D:', 'src', 'KopiQueue', 'docs', '02-design.md') }],
  ['allow', 'Grep', { pattern: 'riverqueue', path: 'docs' }],
  ['allow', 'Bash', { command: 'gh pr list --repo sujanto-gaws/KopiQueue' }],
  ['allow', 'Bash', { command: 'dotnet test --project tests/KopiQueue.Npgsql.Tests' }],
];

let failures = 0;
for (const [expect, tool_name, tool_input] of cases) {
  const run = spawnSync('node', [hook], { input: JSON.stringify({ tool_name, tool_input }), encoding: 'utf8' });
  let got = 'allow';
  if (run.stdout.trim()) {
    got = JSON.parse(run.stdout).hookSpecificOutput.permissionDecision;
  }
  const ok = got === expect && run.status === 0;
  if (!ok) failures++;
  console.log(`${ok ? 'ok   ' : 'WRONG'} ${expect.padEnd(5)} ${tool_name}: ${JSON.stringify(tool_input).slice(0, 90)}`);
}
const garbage = spawnSync('node', [hook], { input: 'not json', encoding: 'utf8' });
console.log(`${garbage.stdout === '' && garbage.status === 0 ? 'ok   ' : 'WRONG'} garbage stdin passes through`);
console.log(failures ? `${failures} failure(s)` : 'all passed');
process.exit(failures ? 1 : 0);
