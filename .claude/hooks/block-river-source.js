// PreToolUse hook: keeps every Claude session in this repo away from River's source code (clean-room, ADR-008,
// CONTRIBUTING.md). It denies a tool call whose input points at River's implementation: source files on GitHub, the
// raw-content and API hosts, clones, the Go module cache, and go commands that fetch, install or run River.
// River's documentation (riverqueue.com/docs) and its README link (github.com/riverqueue/river) stay allowed.
// Reads stdin JSON ({ tool_name, tool_input }); prints a deny decision, or nothing to let the call through.
'use strict';

const BLOCKED = [
  // Source views on GitHub: files, directories, raw files, commits, diffs, pull request files.
  /github\.com\/riverqueue\/[^/\s"'?#]+\/(blob|tree|raw|blame|commits?|compare|pull\/\d+\/(files|commits))\b/i,
  /raw\.githubusercontent\.com\/riverqueue\//i,
  /codeload\.github\.com\/riverqueue\//i,
  /api\.github\.com\/repos\/riverqueue\//i,
  /\brepos\/riverqueue\//i, // gh api repos/riverqueue/...
  /github\.com\/riverqueue\/[^/\s"'?#]+\.git\b/i,
  /\b(git|gh\s+repo)\s+clone\b.*\briverqueue\b/i,
  // Go: the module cache (sources and downloaded zips), and go commands that fetch, show or run River's code.
  /pkg\/mod\/(cache\/download\/)?github\.com\/riverqueue\b/i,
  /\bgo\s+(get|install|run|doc|mod\s+download|list|build|test|vet)\b[^\n]*\briverqueue\b/i,
  // Source browsers and mirrors.
  /sourcegraph\.com\/[^\s"']*riverqueue/i,
  /(gitee|gitlab|bitbucket)\.[a-z.]+\/[^\s"']*riverqueue/i,
];

const WRITE_TOOLS = new Set(['Write', 'Edit', 'MultiEdit', 'NotebookEdit']);

// Every string value in the tool input, however deeply nested.
function strings(value) {
  if (typeof value === 'string') return [value];
  if (Array.isArray(value)) return value.flatMap(strings);
  if (value && typeof value === 'object') return Object.values(value).flatMap(strings);
  return [];
}

let raw = '';
process.stdin.setEncoding('utf8');
process.stdin.on('data', chunk => (raw += chunk));
process.stdin.on('end', () => {
  let input;
  try {
    input = JSON.parse(raw);
  } catch {
    return; // not a tool call we understand: let it through
  }

  const toolInput = input.tool_input ?? {};

  // Writing a file reads nothing, and its content may well mention River (docs, specs, this policy), so only a write
  // tool's target path is checked.
  const values = WRITE_TOOLS.has(input.tool_name)
    ? [toolInput.file_path, toolInput.notebook_path].filter(v => typeof v === 'string')
    : strings(toolInput);

  // Test each line, and each command of a shell line, on its own, so that words in different commands don't add up to
  // a match. Backslashes become slashes so the patterns cover Windows paths too.
  const segments = values.flatMap(v => v.replace(/\\/g, '/').split(/\r?\n|;|&&|\|\||\|/));
  const hit = segments.some(segment => BLOCKED.some(pattern => pattern.test(segment)));
  if (!hit) {
    return;
  }

  process.stdout.write(JSON.stringify({
    hookSpecificOutput: {
      hookEventName: 'PreToolUse',
      permissionDecision: 'deny',
      permissionDecisionReason:
        "Blocked by KopiQueue's clean-room policy (ADR-008, CONTRIBUTING.md): River's source code must not be read, " +
        'fetched, cloned or run. Use River\'s documentation (https://riverqueue.com/docs), its public API names, the ' +
        "recorded goldens and KopiQueue's own design (docs/02-design.md §5.7–5.8 behaviour specs) instead.",
    },
  }));
});
