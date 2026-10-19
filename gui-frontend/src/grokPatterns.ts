// The built-in grok macro catalog `ValidatedField`'s autocomplete suggests
// from, mirrored by hand from `src/format/grok_pattern.rs` (`BUILTINS`).
// Keep it in step: nothing reads this on the backend, so a stale entry
// suggests a macro that does not compile.
//
// Each description is a use case, a match and a near-miss, then the regex
// itself as a reference -- so picking one doesn't require reading regex to
// know whether it is the right one.

import type { StdFuncEntry } from './stdlib';

export const GROK_BUILTIN_PATTERNS: StdFuncEntry[] = [
  {
    name: 'WORD',
    signature: '%{WORD}',
    description:
      'A single alphanumeric token -- a status word, a short id. Matches `error`, `user_42`; not `user-42` (stops at the hyphen). Regex: `\\b\\w+\\b`',
  },
  {
    name: 'NOTSPACE',
    signature: '%{NOTSPACE}',
    description:
      "Anything with no whitespace in it -- for a token whose character set you don't know but that never contains a space, like a path or an id. Matches `/var/log/app.log`; stops at the first space in `hello world`. Regex: `\\S+`",
  },
  {
    name: 'SPACE',
    signature: '%{SPACE}',
    description:
      'Zero or more spaces or tabs -- for optional padding between fields that are not always separated by exactly one space. Matches ``, ` `, `   `. Regex: `\\s*`',
  },
  {
    name: 'INT',
    signature: '%{INT}',
    description:
      'A signed or unsigned whole number -- for any integer field, including a negative one. Matches `42`, `-17`; not `3.14` (stops before the decimal point). Regex: `[+-]?[0-9]+`',
  },
  {
    name: 'POSINT',
    signature: '%{POSINT}',
    description:
      'A positive integer with no leading zero -- for a count or id that is always strictly positive. Matches `1`, `42`; not `0` or `007`. Regex: `[1-9][0-9]*`',
  },
  {
    name: 'BASE10NUM',
    signature: '%{BASE10NUM}',
    description:
      'A signed decimal number, with or without a fractional part -- for any plain numeric value including floats. Matches `42`, `-3.14`, `.5`; not `1e10` (no exponent) or `1,000`. Regex: `[+-]?(?:[0-9]+(?:\\.[0-9]+)?|\\.[0-9]+)`',
  },
  {
    name: 'BASE16NUM',
    signature: '%{BASE16NUM}',
    description:
      'A hex literal with an explicit `0x`/`0X` prefix -- for memory addresses or hex flags written with one. Matches `0xFF`; not a bare `FF` with no prefix. Regex: `0[xX][0-9A-Fa-f]+`',
  },
  {
    name: 'NUMBER',
    signature: '%{NUMBER}',
    description:
      'Alias for BASE10NUM -- use it where the field is naturally called "number". Same matches and misses as BASE10NUM. Regex: `%{BASE10NUM}`',
  },
  {
    name: 'IPV4',
    signature: '%{IPV4}',
    description:
      'A dotted-quad IPv4 address, each octet 0-255 with no leading zero, not joined to a letter, digit or `_`. Matches `10.7.47.56`; not `999.1.1.1` (an octet over 255), `01.2.3.4` (a leading zero) or an IPv6 address. Regex: `(?-u:\\b)(?:(?:25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])\\.){3}(?:25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])(?-u:\\b)`',
  },
  {
    name: 'IPV6',
    signature: '%{IPV6}',
    description:
      'An IPv6 address in any RFC 4291 form, with an optional zone. Matches `2001:db8::1`, `::1`, `::ffff:10.1.2.3`, `fe80::1%eth0`; not `1::2::3`, nine groups, or a plain IPv4 address.',
  },
  {
    name: 'IP',
    signature: '%{IP}',
    description:
      'Either address family -- use this instead of IPV4/IPV6 when a field (a client address, say) could legitimately be either. Matches anything IPV4 or IPV6 matches. Regex: `(?:%{IPV4}|%{IPV6})`',
  },
  {
    name: 'HOSTNAME',
    signature: '%{HOSTNAME}',
    description:
      'A DNS-style hostname or FQDN, dot-separated labels up to 63 characters each. Matches `web-01.internal.example.com`, `localhost`; not a label starting or ending with `-`, or one containing `_`. Regex: `\\b[0-9A-Za-z](?:[0-9A-Za-z-]{0,62}[0-9A-Za-z])?(?:\\.[0-9A-Za-z](?:[0-9A-Za-z-]{0,62}[0-9A-Za-z])?)*\\b`',
  },
  {
    name: 'IPORHOST',
    signature: '%{IPORHOST}',
    description:
      'A host given either as an address or by name -- a client, a server, a peer. Matches `10.0.0.1`, `2001:db8::1`, `web-01.example.com`; not a host with a `:port` or one containing `_`. Regex: `(?:%{IP}|%{HOSTNAME})`',
  },
  {
    name: 'USERNAME',
    signature: '%{USERNAME}',
    description:
      'A login name -- letters, digits, dot, underscore, hyphen. Matches `jane.doe`, `svc_worker-1`; not one containing a space or `@`. Regex: `[a-zA-Z0-9._-]+`',
  },
  {
    name: 'UUID',
    signature: '%{UUID}',
    description:
      'A UUID/GUID in the standard 8-4-4-4-12 hex-with-hyphens form. Matches `550e8400-e29b-41d4-a716-446655440000`; not one with the hyphens removed or a group of the wrong length. Regex: `[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}`',
  },
  {
    name: 'MAC',
    signature: '%{MAC}',
    description:
      'A hardware address, six colon-separated hex pairs. Matches `00:1A:2B:3C:4D:5E`; not the same bytes hyphen-separated (`00-1A-...`). Regex: `(?:[0-9A-Fa-f]{2}:){5}[0-9A-Fa-f]{2}`',
  },
  {
    name: 'DATA',
    signature: '%{DATA}',
    description:
      'Anything, lazily -- matches as little as possible, so use it as filler between two anchors you actually care about: it stops at the first occurrence of whatever comes next in the pattern instead of swallowing the rest of the line. Regex: `.*?`',
  },
  {
    name: 'GREEDYDATA',
    signature: '%{GREEDYDATA}',
    description:
      'Anything, greedily -- matches as much as possible. Use it last, for "everything remaining" like a free-text message; used earlier in a pattern it swallows the text you meant a later macro to capture. Regex: `.*`',
  },
  {
    name: 'QUOTEDSTRING',
    signature: '%{QUOTEDSTRING}',
    description:
      'A double- or single-quoted string, backslash escapes respected. Matches `"GET /index.html HTTP/1.1"`; not an unquoted bare word or a string with an unescaped quote inside it. Regex: `(?:"(?:[^"\\\\]|\\\\.)*"|\'(?:[^\'\\\\]|\\\\.)*\')`',
  },
  {
    name: 'LOGLEVEL',
    signature: '%{LOGLEVEL}',
    description:
      'A log severity word, case-insensitive. Matches `WARN`, `warning`, `Error`, `critical`; not a word outside that set, like `verbose`. Regex: `(?i)(?:trace|debug|info|notice|warn(?:ing)?|err(?:or)?|crit(?:ical)?|fatal|severe|emerg(?:ency)?|alert)`',
  },
  {
    name: 'MONTH',
    signature: '%{MONTH}',
    description:
      'A month name, full or abbreviated, case-insensitive. Matches `Jan`, `january`, `Dec`; not a numeric month like `01`. Regex: `(?i)(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)`',
  },
  {
    name: 'MONTHNUM',
    signature: '%{MONTHNUM}',
    description:
      'A numeric month, 1-12, optional leading zero. Matches `1`, `01`, `12`; not `00` or `13`. Regex: `(?:0?[1-9]|1[0-2])`',
  },
  {
    name: 'MONTHDAY',
    signature: '%{MONTHDAY}',
    description:
      'A numeric day of month, 1-31, optional leading zero. Matches `5`, `05`, `31`; not `00` or `32`. Regex: `(?:0?[1-9]|[12][0-9]|3[01])`',
  },
  {
    name: 'YEAR',
    signature: '%{YEAR}',
    description:
      'A 2- or 4-digit year. Matches `26`, `2026`; a 3-digit run like `202` only takes the first two digits, `20`, and leaves the rest for whatever follows. Regex: `(?:[0-9]{2}){1,2}`',
  },
  {
    name: 'HOUR',
    signature: '%{HOUR}',
    description:
      'A 24-hour hour, 0-23, optional leading zero. Matches `0`, `09`, `23`; not `24` or a 12-hour value with an AM/PM suffix. Regex: `(?:2[0-3]|[01]?[0-9])`',
  },
  {
    name: 'MINUTE',
    signature: '%{MINUTE}',
    description:
      'Two-digit minutes, 00-59. Matches `00`, `59`; not a single digit like `5` or `60`. Regex: `[0-5][0-9]`',
  },
  {
    name: 'SECOND',
    signature: '%{SECOND}',
    description:
      'Seconds, 0-60 (60 for a leap second), with an optional fractional part using `.` or `,`. Matches `07`, `59.999`, `60`; not `61`. Regex: `(?:[0-5]?[0-9](?:[.,][0-9]+)?|60)`',
  },
  {
    name: 'TIME',
    signature: '%{TIME}',
    description:
      'Hour:minute, with an optional :second. Matches `23:59`, `23:59:60.5`; not a 12-hour clock with an AM/PM suffix. Regex: `%{HOUR}:%{MINUTE}(?::%{SECOND})?`',
  },
  {
    name: 'TIMESTAMP_ISO8601',
    signature: '%{TIMESTAMP_ISO8601}',
    description:
      'A full ISO 8601 date-time, `T` or space separator, optional zone offset. Matches `2026-09-14T02:46:00Z`, `2026-09-14 02:46:00+02:00`; not a bare date with no time, or a two-digit year. Regex: `%{YEAR}-%{MONTHNUM}-%{MONTHDAY}[T ]%{TIME}(?:Z|[+-]%{HOUR}:?%{MINUTE})?`',
  },
  {
    name: 'HTTPDATE',
    signature: '%{HTTPDATE}',
    description:
      'The Apache/NCSA common-log date format. Matches `10/Oct/2000:13:55:36 -0700`; not an ISO-formatted date. Regex: `%{MONTHDAY}/%{MONTH}/%{YEAR}:%{TIME} %{INT}`',
  },
  {
    name: 'SYSLOGTIMESTAMP',
    signature: '%{SYSLOGTIMESTAMP}',
    description:
      'The classic syslog timestamp -- no year, a single-digit day space-padded. Matches `Oct 11 22:14:15`, `Oct  1 06:25:24`; not a timestamp that includes a year. Regex: `%{MONTH} +%{MONTHDAY} %{TIME}`',
  },
  {
    name: 'SYSLOGHOST',
    signature: '%{SYSLOGHOST}',
    description:
      'The host field of a syslog line, after its timestamp. Matches `web-01`, `10.0.0.1`; not a host containing a space or `_`. Regex: `%{IPORHOST}`',
  },
  {
    name: 'URIPROTO',
    signature: '%{URIPROTO}',
    description:
      'A URI scheme. Matches `https`, `git+ssh`; not one starting with a digit. Regex: `[A-Za-z][A-Za-z0-9+.-]*`',
  },
  {
    name: 'URIHOST',
    signature: '%{URIHOST}',
    description:
      'A host, optionally with a `:port`. Matches `example.com`, `10.0.0.1:8080`; not a bare IPv6 host without brackets, which this does not support. Regex: `(?:%{IPV4}|%{HOSTNAME})(?::%{POSINT})?`',
  },
  {
    name: 'URIPATH',
    signature: '%{URIPATH}',
    description:
      "A URI path's character set. Matches `/a/b-c_d.e~f`; not a path containing a literal space or `?`, which aren't in the allowed set. Regex: `[A-Za-z0-9$.+!*'(){},~:;=@#%_/-]*`",
  },
  {
    name: 'URIPATHPARAM',
    signature: '%{URIPATHPARAM}',
    description:
      'A URI path with an optional query string. Matches `/search?q=grok`; a trailing `#fragment` after the query is not part of the match. Regex: `%{URIPATH}(?:\\?\\S*)?`',
  },
  {
    name: 'SYSLOGLINE',
    signature: '%{SYSLOGLINE}',
    description:
      'A whole classic syslog line, read into `log.syslog.priority`, `timestamp`, `host.hostname`, `process.name`, `process.pid` and `message`. Matches `<34>Oct 11 22:14:15 mymachine su[230]: failed`.',
  },
  {
    name: 'SYSLOG5424LINE',
    signature: '%{SYSLOG5424LINE}',
    description:
      'A whole RFC 5424 syslog line, matches `<165>1 2003-10-11T22:14:15Z host app - ID47 - An event`, note the version after its priority.',
  },
  {
    name: 'COMMONAPACHELOG',
    signature: '%{COMMONAPACHELOG}',
    description:
      'For a web server\'s access log in the common format. Matches `127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] "GET /a.gif HTTP/1.0" 200 2326`.',
  },
  {
    name: 'COMBINEDAPACHELOG',
    signature: '%{COMBINEDAPACHELOG}',
    description:
      'For a web server\'s access log in the combined format, the common one plus the referrer and user agent. Matches `127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] "GET /a.gif HTTP/1.0" 200 2326 "http://example.com/" "Mozilla/4.08"`.',
  },
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

/** A built-in's definition as its description quotes it, or null for one defined by code rather than a regex. */
export function builtinRegex(entry: StdFuncEntry): string | null {
  return /Regex: `([^`]*)`$/.exec(entry.description)?.[1] ?? null;
}
