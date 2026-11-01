// The built-in grok macro catalog `ValidatedField`'s autocomplete suggests
// from, mirrored by hand from `src/format/grok_pattern.rs` (`BUILTINS`).
// Keep it in step: `gui_grok_sync` there checks each name and each quoted
// regex against the engine's.
//
// Each gives what it is for, values it takes and refuses, and the regex
// itself as a reference, so picking one doesn't require reading regex to
// know whether it is the right one.

import type { StdFuncEntry } from './stdlib';
import { escape } from '../../website/src/lib/highlight/highlight';

/** A built-in pattern as its suggestion shows it: what it is for, values it takes and refuses whole, and its regex. */
function builtin(
  name: string,
  description: string,
  matches: string[],
  misses: string[],
  regex: string,
): StdFuncEntry {
  return { name, signature: `%{${name}}`, description, matches, misses, regex };
}

export const GROK_BUILTIN_PATTERNS: StdFuncEntry[] = [
  builtin(
    'WORD',
    'A single word of letters, digits and `_`, such as a status or a short id.',
    ['error', 'user_42'],
    ['user-42'],
    '\\b\\w+\\b',
  ),
  builtin(
    'NOTSPACE',
    "Anything with no whitespace, for a token such as a path or an id whose characters you don't know.",
    ['/var/log/app.log'],
    ['hello world'],
    '\\S+',
  ),
  builtin(
    'SPACE',
    'Any whitespace, or none, for padding between fields that varies.',
    [],
    [],
    '\\s*',
  ),
  builtin('INT', 'A whole number, with an optional sign.', ['42', '-17'], ['3.14'], '[+-]?[0-9]+'),
  builtin(
    'POSINT',
    'A positive whole number with no leading zero, such as a count or an id.',
    ['1', '42'],
    ['0', '007'],
    '[1-9][0-9]*',
  ),
  builtin(
    'BASE10NUM',
    'A decimal number, with an optional sign and fraction.',
    ['42', '-3.14', '.5'],
    ['1e10', '1,000'],
    '[+-]?(?:[0-9]+(?:\\.[0-9]+)?|\\.[0-9]+)',
  ),
  builtin(
    'BASE16NUM',
    'A hexadecimal number written with `0x` or `0X`.',
    ['0xFF'],
    ['FF'],
    '0[xX][0-9A-Fa-f]+',
  ),
  builtin(
    'NUMBER',
    'The same as BASE10NUM, for a field you would call a number.',
    ['42', '-3.14'],
    ['1e10'],
    '%{BASE10NUM}',
  ),
  builtin(
    'IPV4',
    'A dotted IPv4 address, each part 0 to 255 with no leading zero.',
    ['10.7.47.56'],
    ['999.1.1.1', '01.2.3.4'],
    '(?-u:\\b)(?:(?:25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])\\.){3}(?:25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])(?-u:\\b)',
  ),
  builtin(
    'IPV6',
    'An IPv6 address in any RFC 4291 form, with an optional zone.',
    ['2001:db8::1', '::1', '::ffff:10.1.2.3', 'fe80::1%eth0'],
    ['1::2::3'],
    '(?:(?:(?:[0-9A-Fa-f]{1,4}:){6}|(?:[0-9A-Fa-f]{1,4}:){5}:|(?:[0-9A-Fa-f]{1,4}:){4}:(?:[0-9A-Fa-f]{1,4}:){0,1}|(?:[0-9A-Fa-f]{1,4}:){3}:(?:[0-9A-Fa-f]{1,4}:){0,2}|(?:[0-9A-Fa-f]{1,4}:){2}:(?:[0-9A-Fa-f]{1,4}:){0,3}|[0-9A-Fa-f]{1,4}::(?:[0-9A-Fa-f]{1,4}:){0,4}|::(?:[0-9A-Fa-f]{1,4}:){0,5})%{IPV4}|(?:[0-9A-Fa-f]{1,4}:){7}[0-9A-Fa-f]{1,4}|(?:[0-9A-Fa-f]{1,4}:){7}:|(?:[0-9A-Fa-f]{1,4}:){6}:(?:[0-9A-Fa-f]{1,4})?|(?:[0-9A-Fa-f]{1,4}:){5}:(?:[0-9A-Fa-f]{1,4}(?::[0-9A-Fa-f]{1,4}){0,1})?|(?:[0-9A-Fa-f]{1,4}:){4}:(?:[0-9A-Fa-f]{1,4}(?::[0-9A-Fa-f]{1,4}){0,2})?|(?:[0-9A-Fa-f]{1,4}:){3}:(?:[0-9A-Fa-f]{1,4}(?::[0-9A-Fa-f]{1,4}){0,3})?|(?:[0-9A-Fa-f]{1,4}:){2}:(?:[0-9A-Fa-f]{1,4}(?::[0-9A-Fa-f]{1,4}){0,4})?|[0-9A-Fa-f]{1,4}::(?:[0-9A-Fa-f]{1,4}(?::[0-9A-Fa-f]{1,4}){0,5})?|::(?:[0-9A-Fa-f]{1,4}(?::[0-9A-Fa-f]{1,4}){0,6})?)(?:%[0-9A-Za-z._~-]+)?',
  ),
  builtin(
    'IP',
    'An IPv4 or IPv6 address, for a field that can hold either.',
    ['10.0.0.1', '2001:db8::1'],
    ['999.1.1.1'],
    '(?:%{IPV4}|%{IPV6})',
  ),
  builtin(
    'HOSTNAME',
    'A hostname or FQDN: dot-separated labels of up to 63 letters, digits, `_` or `-`. A label cannot start or end with `-`, and the name may end with `.`.',
    ['web-01.internal.example.com', 'localhost', 'build_01', 'example.com.'],
    ['-web', 'web-'],
    '\\b[0-9A-Za-z_](?:[0-9A-Za-z_-]{0,62}[0-9A-Za-z_])?(?:\\.[0-9A-Za-z_](?:[0-9A-Za-z_-]{0,62}[0-9A-Za-z_])?)*\\b\\.?',
  ),
  builtin(
    'IPORHOST',
    'An IP address or a hostname, for a host such as a client, a server or a peer.',
    ['10.0.0.1', '2001:db8::1', 'web-01.example.com'],
    ['web-01:8080'],
    '(?:%{IP}|%{HOSTNAME})',
  ),
  builtin(
    'USERNAME',
    'A login name of letters, digits, `.`, `_` and `-`, or a Windows account written with `\\`, `@` or `$`.',
    ['jane.doe', 'svc_worker-1', 'CORP\\jdoe', 'jdoe@corp.com', 'WS01$'],
    ['jane doe'],
    '[a-zA-Z0-9._\\\\@$-]+',
  ),
  builtin(
    'UUID',
    'A UUID or GUID in its 8-4-4-4-12 hexadecimal form, in braces or not, or as 32 hexadecimal digits.',
    [
      '550e8400-e29b-41d4-a716-446655440000',
      '{550e8400-e29b-41d4-a716-446655440000}',
      '550e8400e29b41d4a716446655440000',
    ],
    ['550e8400-e29b-41d4-a716-44665544000'],
    '(?:\\{[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}\\}|[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}|\\b[A-Fa-f0-9]{32}\\b)',
  ),
  builtin(
    'MAC',
    'A hardware address: pairs split by colons or dashes, groups of four split by dots, two halves split by a dash, or no separator.',
    ['00:1A:2B:3C:4D:5E', '00-1A-2B-3C-4D-5E', '001A.2B3C.4D5E', '001A2B-3C4D5E', '001A2B3C4D5E'],
    ['00:1A:2B:3C:4D'],
    '(?:(?:[0-9A-Fa-f]{2}:){5}[0-9A-Fa-f]{2}|(?:[0-9A-Fa-f]{2}-){5}[0-9A-Fa-f]{2}|(?:[0-9A-Fa-f]{4}\\.){2}[0-9A-Fa-f]{4}|[0-9A-Fa-f]{6}-[0-9A-Fa-f]{6}|\\b[0-9A-Fa-f]{12}\\b)',
  ),
  builtin(
    'DATA',
    'Anything, as little as possible. Use it between two parts you care about: it stops where the next part of the pattern starts.',
    [],
    [],
    '.*?',
  ),
  builtin(
    'GREEDYDATA',
    'Anything, greedily: matches as much as possible. Use it last, for "everything remaining" like a free-text message; used earlier in a pattern it swallows the text you meant a later macro to capture.',
    [],
    [],
    '.*',
  ),
  builtin(
    'QUOTEDSTRING',
    'A string in double, single or back quotes, with backslash escapes. The quotes are part of the match.',
    ['"GET /index.html HTTP/1.1"', "'it\\'s'", '`date`'],
    ['unquoted', '"open'],
    '(?:"(?:[^"\\\\]|\\\\.)*"|\'(?:[^\'\\\\]|\\\\.)*\'|`(?:[^`\\\\]|\\\\.)*`)',
  ),
  builtin(
    'LOGLEVEL',
    'A log level name, in any case.',
    ['WARN', 'warning', 'Error', 'Information', 'verbose', 'panic', 'FINEST'],
    ['informational', 'warn!'],
    '(?i)(?:trace|debug|dbg|info(?:rmation)?|inf|notice|warn(?:ing)?|wrn|err(?:or)?|crit(?:ical)?|fatal|severe|emerg(?:ency)?|alert|panic|verbose|fine(?:r|st)?)',
  ),
  builtin(
    'MONTH',
    'A month name, in full or as three letters, in any case.',
    ['Jan', 'january', 'Dec'],
    ['01', 'Sept'],
    '(?i)(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)',
  ),
  builtin(
    'MONTHNUM',
    'A month number, 1 to 12, with an optional leading zero.',
    ['1', '01', '12'],
    ['00', '13'],
    '(?:0?[1-9]|1[0-2])',
  ),
  builtin(
    'MONTHDAY',
    'A day of the month, 1 to 31, with an optional leading zero.',
    ['5', '05', '31'],
    ['00', '32'],
    '(?:0?[1-9]|[12][0-9]|3[01])',
  ),
  builtin('YEAR', 'A year of two or four digits.', ['26', '2026'], ['2'], '(?:[0-9]{2}){1,2}'),
  builtin(
    'HOUR',
    'An hour on a 24-hour clock, 0 to 23, with an optional leading zero.',
    ['0', '09', '23'],
    ['24'],
    '(?:2[0-3]|[01]?[0-9])',
  ),
  builtin('MINUTE', 'Minutes as two digits, 00 to 59.', ['00', '59'], ['5', '60'], '[0-5][0-9]'),
  builtin(
    'SECOND',
    'Seconds, 0 to 60 (60 for a leap second), with an optional fraction after `.` or `,`.',
    ['07', '59.999', '07,5', '60'],
    ['61'],
    '(?:[0-5]?[0-9](?:[.,][0-9]+)?|60)',
  ),
  builtin(
    'TIME',
    'Hours and minutes, with optional seconds.',
    ['23:59', '23:59:60'],
    ['12', '24:00'],
    '%{HOUR}:%{MINUTE}(?::%{SECOND})?',
  ),
  builtin(
    'TIMESTAMP_ISO8601',
    'An ISO 8601 date and time, with `T` or a space between them and an optional zone. The colons in the time are optional.',
    ['2026-09-14T02:46:00Z', '2026-09-14 02:46:00+02:00', '2026-09-14T024600Z'],
    ['2026-09-14'],
    '%{YEAR}-%{MONTHNUM}-%{MONTHDAY}[T ]%{HOUR}:?%{MINUTE}(?::?%{SECOND})?(?:Z|[+-]%{HOUR}:?%{MINUTE})?',
  ),
  builtin(
    'HTTPDATE',
    "A web server log's date, as Apache and NCSA write it.",
    ['10/Oct/2000:13:55:36 -0700'],
    ['2000-10-10T13:55:36'],
    '%{MONTHDAY}/%{MONTH}/%{YEAR}:%{TIME} %{INT}',
  ),
  builtin(
    'SYSLOGTIMESTAMP',
    'A classic syslog timestamp, with no year. A one-digit day is padded with a space.',
    ['Oct 11 22:14:15', 'Oct  1 06:25:24'],
    ['2026 Oct 11 22:14:15'],
    '%{MONTH} +%{MONTHDAY} %{TIME}',
  ),
  builtin(
    'SYSLOGHOST',
    'The host in a syslog line, after its timestamp.',
    ['web-01', '10.0.0.1'],
    ['web 01'],
    '%{IPORHOST}',
  ),
  builtin('URIPROTO', 'A URI scheme.', ['https', 'git+ssh'], ['1http'], '[A-Za-z][A-Za-z0-9+.-]*'),
  builtin(
    'URIHOST',
    'A host, with an optional `:port`. An IPv6 address is written in brackets.',
    ['example.com', '10.0.0.1:8080', '[2001:db8::1]:8443'],
    ['example.com:port'],
    '(?:%{IPV4}|\\[%{IPV6}\\]|%{HOSTNAME})(?::%{POSINT})?',
  ),
  builtin(
    'URIPATH',
    'A URI path. It stops at a space and at `?`.',
    ['/a/b-c_d.e~f', '/a&b'],
    ['/a b'],
    "[A-Za-z0-9$.+!*'(){},~:;=@#%&\\[\\]|_/-]*",
  ),
  builtin(
    'URIPATHPARAM',
    'A URI path with an optional `?query`.',
    ['/search?q=grok'],
    ['/a b'],
    '%{URIPATH}(?:\\?\\S*)?',
  ),
  builtin(
    'SYSLOGLINE',
    'A whole syslog line, with a classic or ISO 8601 timestamp, read into `log.syslog.priority`, `timestamp`, `host.hostname`, `process.name`, `process.pid` and `message`.',
    ['<34>Oct 11 22:14:15 mymachine su[230]: failed'],
    [],
    '(?:<(?P<log.syslog.priority>[0-9]{1,3})>)?(?P<timestamp>%{SYSLOGTIMESTAMP}|%{TIMESTAMP_ISO8601}) %{NOTSPACE:host.hostname}(?: (?P<process.name>[\\x21-\\x5a\\x5c\\x5e-\\x7e]+)(?:\\[%{POSINT:process.pid}\\])?:)? %{GREEDYDATA:message}',
  ),
  builtin(
    'SYSLOG5424LINE',
    'A whole RFC 5424 syslog line, with its version after the priority.',
    ['<165>1 2003-10-11T22:14:15Z host app - ID47 - An event'],
    [],
    '<(?P<log.syslog.priority>[0-9]{1,3})>%{POSINT:log.syslog.version} (?:-|%{TIMESTAMP_ISO8601:timestamp}) (?:-|%{NOTSPACE:host.hostname}) (?:-|%{NOTSPACE:log.syslog.appname}) (?:-|%{NOTSPACE:log.syslog.procid}) (?:-|%{NOTSPACE:log.syslog.msgid}) (?:-|(?P<log.syslog.structured_data>(?:\\[(?:[^\\]"\\\\]|\\\\.|"(?:[^"\\\\]|\\\\.)*")*\\])+))(?: %{GREEDYDATA:message})?',
  ),
  builtin(
    'COMMONAPACHELOG',
    "A web server's access log line in the common format.",
    ['127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] "GET /a.gif HTTP/1.0" 200 2326'],
    [],
    '%{IPORHOST:source.address} (?:-|%{NOTSPACE:apache.access.user.identity}) (?:-|%{NOTSPACE:user.name}) \\[%{HTTPDATE:timestamp}\\] "(?:(?P<http.request.method>[!#$%&\'*+.^_`|~0-9A-Za-z-]+) %{NOTSPACE:url.original}(?: HTTP/%{NUMBER:http.version})?|%{DATA})" (?P<http.response.status_code>[0-9]{3}) (?:(?P<http.response.body.bytes>[0-9]+)|-)',
  ),
  builtin(
    'COMBINEDAPACHELOG',
    "A web server's access log line in the combined format: the common one, then the referrer and the user agent.",
    [
      '127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] "GET /a.gif HTTP/1.0" 200 2326 "http://example.com/" "Mozilla/4.08"',
    ],
    [],
    '%{COMMONAPACHELOG} "(?:-|%{DATA:http.request.referrer})" "(?:-|%{DATA:user_agent.original})"',
  ),
];

/** The bundle's own patterns as suggestions, after the built-ins: each described by its definition, which is all
 * there is to say about it. */
export function sharedGrokSuggestions(patterns: { id: string; pattern: string }[]): StdFuncEntry[] {
  return patterns
    .filter((p) => p.id !== '')
    .map((p) => ({ name: p.id, signature: `%{${p.id}}`, description: p.pattern }));
}

/** Why a bundle's pattern may not take `name`, as the leader would refuse it, or null when it may. */
export function grokNameProblem(name: string, taken: Iterable<string>): string | null {
  if (name.trim() === '') return 'a name is required';
  if (!/^[A-Za-z0-9_]+$/.test(name)) return 'use letters, digits and underscores';
  if (GROK_BUILTIN_PATTERNS.some((b) => b.name === name)) return `${name} is a built-in pattern`;
  for (const t of taken) if (t === name) return `${name} is taken`;
  return null;
}

/** A built-in's definition, or null for an entry that has none. */
export function builtinRegex(entry: StdFuncEntry): string | null {
  return entry.regex ?? null;
}

/** A macro, a named group's opening, a character class, an escape, a group's flags, or an operator. */
export const GROK_TOKEN =
  /%\{([A-Za-z0-9_]+)(?::([^}]*))?\}|\(\?P?<([^>]*)>|\[\^?\]?(?:\\.|[^\]\\])*\]|\\.|\(\?[A-Za-z-]*:?|[?*+|^$]|\{\d+(?:,\d*)?\}/g;

/** `text`, a grok pattern, as escaped HTML: each macro's name as a function and its field as a key, a named group's
 * name as a key, a character class as a string, an escape as a literal, and a quantifier, alternation, anchor or
 * group flag as a keyword. */
export function highlightGrok(text: string): string {
  const span = (kind: string, s: string) => `<span class="tok-${kind}">${escape(s)}</span>`;
  let out = '';
  let from = 0;
  for (const m of text.matchAll(GROK_TOKEN)) {
    const at = m.index ?? 0;
    out += escape(text.slice(from, at));
    const [token, macro, field, group] = m;
    if (macro !== undefined)
      out +=
        '%{' +
        span('function', macro) +
        (field !== undefined ? ':' + span('key', field) : '') +
        '}';
    else if (group !== undefined)
      out += escape(token.slice(0, token.length - group.length - 1)) + span('key', group) + '&gt;';
    else if (token.startsWith('[')) out += span('string', token);
    else if (token.startsWith('\\')) out += span('literal', token);
    else out += span('keyword', token);
    from = at + token.length;
  }
  return out + escape(text.slice(from));
}
