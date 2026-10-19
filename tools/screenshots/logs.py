"""Synthetic log lines in the shapes of Elastic's integration test logs.

Each generator takes a random.Random and a datetime and returns one raw line.
The shapes follow the sample logs in elastic/integrations:
  panw/data_stream/panos        (PAN-OS 10.x TRAFFIC over syslog)
  fortinet_fortigate/.../log    (FortiOS key=value traffic)
  nginx/data_stream/access      (combined access log)
  system/data_stream/auth       (sshd and sudo in auth.log)
  aws/data_stream/cloudtrail    (one CloudTrail event per message)
  cisco_asa/data_stream/log     (ASA connection build and teardown over syslog)
Addresses outside 10/8 come from the MaxMind test ranges those logs use.
"""

import json
import math
import uuid

MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

# MaxMind GeoIP2 test-database ranges, as used throughout Elastic's samples.
PUBLIC = [
    "81.2.69.142", "81.2.69.144", "81.2.69.145", "81.2.69.160", "81.2.69.193",
    "89.160.20.112", "89.160.20.113", "89.160.20.128", "89.160.20.156",
    "175.16.199.1", "175.16.199.24", "175.16.199.60",
    "216.160.83.56", "216.160.83.58", "216.160.83.61",
    "67.43.156.13", "67.43.156.14", "67.43.156.44",
    "2.125.160.216", "2.125.160.218",
]
COUNTRY = {
    "81.2": "United Kingdom", "89.160": "Sweden", "175.16": "China",
    "216.160": "United States", "67.43": "Bhutan", "2.125": "United Kingdom",
}


def country(ip):
    for prefix, name in COUNTRY.items():
        if ip.startswith(prefix + "."):
            return name
    return "United States"


# -- directory ---------------------------------------------------------------

USERS = [
    ("jchen", "Engineering", "Jia Chen"), ("mgarcia", "Finance", "Marta Garcia"),
    ("apatel", "Engineering", "Arjun Patel"), ("sokafor", "Sales", "Sade Okafor"),
    ("lnguyen", "Support", "Linh Nguyen"), ("rkowalski", "IT Operations", "Robert Kowalski"),
    ("tjohansson", "Engineering", "Tove Johansson"), ("hsato", "Marketing", "Hiro Sato"),
    ("dmurphy", "Legal", "Dana Murphy"), ("eromero", "Sales", "Elena Romero"),
    ("kbrown", "Finance", "Kevin Brown"), ("fnovak", "IT Operations", "Filip Novak"),
    ("ysilva", "Support", "Yara Silva"), ("bwright", "Executive", "Beth Wright"),
    ("nhassan", "Engineering", "Nadia Hassan"), ("clopez", "Marketing", "Carlos Lopez"),
    ("pmueller", "Security", "Paul Mueller"), ("ikim", "Engineering", "Ivy Kim"),
    ("omensah", "Sales", "Owen Mensah"), ("vrossi", "HR", "Vera Rossi"),
]

# 10.<n>.0.0/16 per site.
SITES = [
    (10, "nyc-hq", "New York", "restricted"), (11, "nyc-hq", "New York", "general"),
    (20, "lon-office", "London", "general"), (30, "fra-dc1", "Frankfurt", "restricted"),
    (40, "sin-office", "Singapore", "general"), (50, "sfo-office", "San Francisco", "general"),
    (60, "dub-dc2", "Dublin", "restricted"), (70, "tor-office", "Toronto", "general"),
]

# PAN-OS App-ID: name, port, transport, category, risk (1-5), subcategory.
APPS = [
    ("ssl", 443, "tcp", "networking", 4, "encrypted-tunnel", 18),
    ("web-browsing", 80, "tcp", "general-internet", 4, "internet-utility", 8),
    ("dns-base", 53, "udp", "networking", 3, "infrastructure", 14),
    ("ms-office365-base", 443, "tcp", "business-systems", 2, "office-programs", 10),
    ("ms-teams", 443, "tcp", "collaboration", 2, "instant-messaging", 6),
    ("slack-base", 443, "tcp", "collaboration", 2, "instant-messaging", 5),
    ("zoom-base", 443, "tcp", "collaboration", 2, "voip-video", 4),
    ("github-base", 443, "tcp", "business-systems", 2, "software-development", 4),
    ("salesforce-base", 443, "tcp", "business-systems", 2, "crm", 3),
    ("google-base", 443, "tcp", "general-internet", 4, "internet-utility", 6),
    ("ssh", 22, "tcp", "networking", 4, "encrypted-tunnel", 2),
    ("ntp-base", 123, "udp", "networking", 2, "infrastructure", 2),
    ("ldap", 389, "tcp", "business-systems", 3, "auth-service", 2),
    ("kerberos", 88, "tcp", "business-systems", 2, "auth-service", 2),
    ("smtp-base", 25, "tcp", "collaboration", 5, "email", 1),
    ("bittorrent", 6881, "tcp", "general-internet", 5, "file-sharing", 1),
    ("tor", 9001, "tcp", "networking", 5, "encrypted-tunnel", 1),
]
APP_WEIGHTS = [a[6] for a in APPS]
DENIED = {"bittorrent", "tor", "smtp-base"}

FIREWALLS = [
    ("PA-NYC-01", "013201001234", 10), ("PA-NYC-02", "013201001235", 11),
    ("PA-LON-01", "013201004410", 20), ("PA-FRA-01", "013201007721", 30),
]


def _session(r):
    return r.randint(100000, 999999)


def panos_traffic(r, now):
    """One PAN-OS TRAFFIC log: a BSD syslog envelope around the vendor's CSV."""
    device, serial, net = r.choice(FIREWALLS)
    app, port, proto, category, risk, _sub, _w = r.choices(APPS, weights=APP_WEIGHTS)[0]
    dmz = r.random() < 0.12
    src = f"172.16.{r.randint(1, 20)}.{r.randint(2, 250)}" if dmz else f"10.{net}.{r.randint(0, 40)}.{r.randint(2, 250)}"
    dst = r.choice(PUBLIC)
    nat = f"198.51.100.{r.randint(2, 30)}"
    user = "" if dmz or r.random() < 0.25 else "acme\\" + r.choice(USERS)[0]
    deny = app in DENIED or (dmz and r.random() < 0.15)
    rule = ("block-high-risk-apps" if app in DENIED else "dmz-deny-default") if deny else (
        "dmz-to-internet" if dmz else ("allow-infrastructure" if category == "networking" and app != "ssl" else "allow-outbound-web"))
    action = "deny" if deny else "allow"
    subtype = "deny" if deny else "end"
    sent = 0 if deny else int(r.lognormvariate(7.5, 1.1))
    recv = 0 if deny else int(r.lognormvariate(8.6, 1.5))
    psent = 1 if deny else max(2, sent // 700)
    precv = 0 if deny else max(1, recv // 1100)
    elapsed = 0 if deny else int(r.expovariate(1 / 25))
    start = now.timestamp() - elapsed
    t = now.strftime("%Y/%m/%d %H:%M:%S")
    st = type(now).fromtimestamp(start, now.tzinfo).strftime("%Y/%m/%d %H:%M:%S")
    reason = "policy-deny" if deny else ("aged-out" if proto == "udp" else r.choice(["tcp-fin", "tcp-fin", "tcp-fin", "tcp-rst-from-client"]))
    szone, dzone = ("dmz", "untrust") if dmz else ("trust", "untrust")
    iif = "ethernet1/3" if dmz else "ethernet1/2"
    syslog = f"{MONTHS[now.month - 1]} {now.day:>2} {now:%H:%M:%S}"
    fields = [
        "1", t, serial, "TRAFFIC", subtype, "2562", t,
        src, dst, nat, dst, rule, user, "", app, "vsys1", szone, dzone, iif, "ethernet1/1",
        "log-fwd-siem", t, str(_session(r)), "1", str(r.randint(32768, 60999)), str(port),
        str(r.randint(20000, 60000)), str(port), "0x400053", proto, action,
        str(sent + recv), str(sent), str(recv), str(psent + precv), st, str(elapsed),
        "computer-and-internet-info" if category != "business-systems" else "business-and-economy",
        "", str(r.randint(10**15, 10**16)), "0x8000000000000000",
        "10.0.0.0-10.255.255.255" if not dmz else "172.16.0.0-172.31.255.255", country(dst), "",
        str(psent), str(precv), reason, "0", "0", "0", "0", "", device.lower(), "from-policy",
        "", "", "0", "", "0", "", "N/A", "0", "0", "0", "0", str(uuid.UUID(int=r.getrandbits(128))), "0", "0",
    ]
    return f"<14>{syslog} {device.lower()}.acme.example {','.join(fields)}"


# -- FortiGate -----------------------------------------------------------------

FORTI_APPS = [
    ("HTTPS.BROWSER", "Web.Client", "medium", "HTTPS", 443, 6, 20),
    ("Microsoft.Teams", "Collaboration", "elevated", "HTTPS", 443, 6, 10),
    ("Google.Services", "General.Interest", "medium", "HTTPS", 443, 6, 8),
    ("DNS", "Network.Service", "elevated", "DNS", 53, 17, 12),
    ("HTTP.BROWSER_Chrome", "Web.Client", "medium", "HTTP", 80, 6, 6),
    ("Dropbox", "Storage.Backup", "elevated", "HTTPS", 443, 6, 4),
    ("Dailymotion", "Video/Audio", "elevated", "HTTPS", 443, 6, 3),
    ("SSH", "Remote.Access", "elevated", "SSH", 22, 6, 2),
    ("NTP", "Network.Service", "low", "NTP", 123, 17, 3),
]
FORTI_WEIGHTS = [a[6] for a in FORTI_APPS]
FORTIGATES = [("FGT-DUB-01", "FG100FTK20001234", 60), ("FGT-TOR-01", "FG100FTK20005678", 70),
              ("FGT-SIN-01", "FG60FTK21009911", 40)]


PROTO_NAMES = {1: "icmp", 6: "tcp", 17: "udp"}


def fortigate_traffic(r, now, drift=False):
    """With `drift`, FGT-SIN-01 names `proto` (`tcp`) where the others number it (`6`), as a firmware that changed it
    would."""
    dev, devid, net = r.choice(FORTIGATES)
    app, appcat, apprisk, service, port, proto, _w = r.choices(FORTI_APPS, weights=FORTI_WEIGHTS)[0]
    src = f"10.{net}.{r.randint(0, 30)}.{r.randint(2, 250)}"
    dst = r.choice(PUBLIC)
    deny = r.random() < 0.06
    policyid, policyname = (0, "implicit-deny") if deny else ((12, "LAN-to-Internet") if port != 22 else (31, "Admin-SSH-Out"))
    action = "deny" if deny else r.choice(["close", "close", "close", "timeout"] if proto == 6 else ["accept"])
    dur = 0 if deny else int(r.expovariate(1 / 30))
    sent = 0 if deny else int(r.lognormvariate(7.3, 1.2))
    rcvd = 0 if deny else int(r.lognormvariate(8.4, 1.6))
    ev = int(now.timestamp() * 1e9) + r.randint(0, 999999)
    kv = [
        f"date={now:%Y-%m-%d}", f"time={now:%H:%M:%S}", f'devname="{dev}"', f'devid="{devid}"',
        f"eventtime={ev}", 'tz="+0000"', 'logid="0000000013"', 'type="traffic"', 'subtype="forward"',
        'level="notice"', 'vd="root"', f"srcip={src}", f"srcport={r.randint(32768, 60999)}",
        'srcintf="internal"', 'srcintfrole="lan"', f"dstip={dst}", f"dstport={port}", 'dstintf="wan1"',
        'dstintfrole="wan"', 'srccountry="Reserved"', f'dstcountry="{country(dst)}"',
        f"sessionid={r.randint(10**6, 10**8)}",
        f'proto="{PROTO_NAMES[proto]}"' if drift and dev == "FGT-SIN-01" else f"proto={proto}", f'action="{action}"', f"policyid={policyid}",
        'policytype="policy"', f'policyname="{policyname}"', f'service="{service}"', 'trandisp="snat"',
        f"transip=198.51.100.{r.randint(40, 60)}", f"transport={r.randint(20000, 60000)}",
        f'app="{app}"', f'appcat="{appcat}"', f'apprisk="{apprisk}"', f"duration={dur}",
        f"sentbyte={sent}", f"rcvdbyte={rcvd}", f"sentpkt={max(1, sent // 800)}",
        f"rcvdpkt={0 if deny else max(1, rcvd // 1200)}",
    ]
    if r.random() < 0.7 and not deny:
        u = r.choice(USERS)[0]
        kv.append(f'user="{u}"')
    return " ".join(kv)


# -- Cisco ASA ---------------------------------------------------------------------

ASA_FIREWALLS = [("colo-asa-01", 60), ("colo-asa-02", 61)]
ASA_SERVICES = [("TCP", 443, 10), ("TCP", 80, 3), ("TCP", 22, 1), ("UDP", 53, 5), ("UDP", 123, 1)]
ASA_WEIGHTS = [s[2] for s in ASA_SERVICES]


def asa_connection(r, now):
    """One ASA connection built (302013/302015) or torn down (302014/302016), outbound from inside."""
    host, net = r.choice(ASA_FIREWALLS)
    proto, port, _w = r.choices(ASA_SERVICES, weights=ASA_WEIGHTS)[0]
    conn = r.randint(10**6, 10**9)
    src, sport = f"10.{net}.{r.randint(0, 20)}.{r.randint(2, 250)}", r.randint(32768, 60999)
    dst = r.choice(PUBLIC)
    mapped = f"198.51.100.{r.randint(70, 90)}/{r.randint(20000, 60000)}"
    stamp = f"{MONTHS[now.month - 1]} {now.day:>2} {now:%H:%M:%S}"
    if r.random() < 0.5:
        mid = 302013 if proto == "TCP" else 302015
        msg = (f"Built outbound {proto} connection {conn} for outside:{dst}/{port} ({dst}/{port}) "
               f"to inside:{src}/{sport} ({mapped})")
    else:
        mid = 302014 if proto == "TCP" else 302016
        secs = int(r.expovariate(1 / 40))
        tail = f" {r.choice(['TCP FINs', 'TCP FINs', 'TCP Reset-I', 'TCP Reset-O'])}" if proto == "TCP" else ""
        msg = (f"Teardown {proto} connection {conn} for outside:{dst}/{port} to inside:{src}/{sport} "
               f"duration {secs // 3600}:{secs // 60 % 60:02}:{secs % 60:02} bytes {int(r.lognormvariate(8, 1.5))}{tail}")
    return f"{stamp} {host} %ASA-6-{mid}: {msg}"


# -- nginx ---------------------------------------------------------------------

UAS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64; rv:130.0) Gecko/20100101 Firefox/130.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
    "curl/8.9.1",
]
PATHS = [
    ("GET", "/", 200, 6120, 10), ("GET", "/products", 200, 18422, 8), ("GET", "/products/{id}", 200, 9321, 12),
    ("GET", "/api/v1/cart", 200, 812, 10), ("POST", "/api/v1/cart/items", 201, 144, 5),
    ("GET", "/api/v1/orders/{id}", 200, 1320, 5), ("POST", "/api/v1/checkout", 200, 402, 3),
    ("GET", "/static/app.9f3c2e.js", 200, 184233, 8), ("GET", "/static/app.9f3c2e.css", 304, 0, 6),
    ("GET", "/favicon.ico", 404, 571, 2), ("POST", "/login", 302, 0, 2), ("GET", "/account", 200, 7311, 2),
    ("GET", "/healthz", 200, 2, 12),
]
PATH_WEIGHTS = [p[4] for p in PATHS]
WEBHOSTS = ["web-01", "web-02"]


def nginx_access(r, now, error_rate=0.01):
    method, path, status, size, _w = r.choices(PATHS, weights=PATH_WEIGHTS)[0]
    path = path.replace("{id}", str(r.randint(1000, 9999)))
    if path == "/healthz":
        ip, ua, user, ref = f"10.70.0.{r.randint(2, 6)}", "ELB-HealthChecker/2.0", "-", "-"
    else:
        ip, ua = r.choice(PUBLIC), r.choice(UAS)
        user = r.choice(USERS)[0] if path.startswith("/account") else "-"
        ref = "https://shop.acme.example/" if r.random() < 0.5 else "-"
        if r.random() < error_rate and path.startswith("/api"):
            status, size = r.choice([(500, 212), (502, 157), (503, 190)])
        elif r.random() < 0.02:
            status, size = 404, 571
        size = int(size * r.uniform(0.8, 1.2)) if size else 0
    stamp = now.strftime("%d/%b/%Y:%H:%M:%S +0000")
    return f'{ip} - {user} [{stamp}] "{method} {path} HTTP/1.1" {status} {size} "{ref}" "{ua}"'


# -- auth.log --------------------------------------------------------------------

AUTH_HOSTS = ["bastion-01", "bastion-02", "app-db-01"]
ATTACKERS = ["89.160.20.156", "175.16.199.60", "67.43.156.44"]
PROBES = ["root", "admin", "test", "oracle", "postgres", "ubuntu", "git"]


def auth_log(r, now, burst=False):
    host = r.choice(AUTH_HOSTS)
    stamp = f"{MONTHS[now.month - 1]} {now.day:>2} {now:%H:%M:%S}"
    pid = r.randint(1000, 65000)
    x = r.random()
    if burst or x < 0.35:
        ip = r.choice(ATTACKERS)
        who = r.choice(PROBES)
        if r.random() < 0.5:
            msg = f"sshd[{pid}]: Failed password for {'invalid user ' if who not in ('root',) else ''}{who} from {ip} port {r.randint(30000, 65000)} ssh2"
        else:
            msg = f"sshd[{pid}]: Invalid user {who} from {ip} port {r.randint(30000, 65000)}"
    elif x < 0.70:
        u = r.choice(USERS)[0]
        how = r.choice(["publickey", "publickey", "password"])
        tail = f": ED25519 SHA256:{uuid.UUID(int=r.getrandbits(128)).hex[:43]}" if how == "publickey" else ""
        msg = f"sshd[{pid}]: Accepted {how} for {u} from 10.{r.choice(SITES)[0]}.{r.randint(0, 40)}.{r.randint(2, 250)} port {r.randint(30000, 65000)} ssh2{tail}"
    elif x < 0.88:
        u = r.choice(USERS)[0]
        cmd = r.choice(["/usr/bin/systemctl restart nginx", "/usr/bin/journalctl -u smeltr", "/bin/cat /var/log/secure",
                        "/usr/bin/apt-get upgrade", "/usr/bin/docker ps"])
        msg = f"sudo:   {u} : TTY=pts/{r.randint(0, 4)} ; PWD=/home/{u} ; USER=root ; COMMAND={cmd}"
    else:
        u = r.choice(USERS)[0]
        msg = f"sshd[{pid}]: pam_unix(sshd:session): session closed for user {u}"
    return f"{stamp} {host} {msg}"


# -- CloudTrail ------------------------------------------------------------------

REGIONS = ["us-east-1", "us-east-1", "us-west-2", "eu-west-1"]
CT_EVENTS = [
    ("signin.amazonaws.com", "ConsoleLogin", "AwsConsoleSignIn", 6),
    ("sts.amazonaws.com", "AssumeRole", "AwsApiCall", 18),
    ("ec2.amazonaws.com", "DescribeInstances", "AwsApiCall", 16),
    ("s3.amazonaws.com", "GetBucketAcl", "AwsApiCall", 8),
    ("s3.amazonaws.com", "PutBucketPolicy", "AwsApiCall", 2),
    ("iam.amazonaws.com", "CreateAccessKey", "AwsApiCall", 2),
    ("iam.amazonaws.com", "AttachRolePolicy", "AwsApiCall", 2),
    ("ec2.amazonaws.com", "AuthorizeSecurityGroupIngress", "AwsApiCall", 3),
    ("kms.amazonaws.com", "Decrypt", "AwsApiCall", 14),
    ("lambda.amazonaws.com", "Invoke", "AwsApiCall", 8),
    ("secretsmanager.amazonaws.com", "GetSecretValue", "AwsApiCall", 6),
]
CT_WEIGHTS = [e[3] for e in CT_EVENTS]


def cloudtrail(r, now):
    source, name, etype, _w = r.choices(CT_EVENTS, weights=CT_WEIGHTS)[0]
    u = r.choice(USERS)
    account = "111111111111" if r.random() < 0.7 else "222222222222"
    iam_user = r.random() < 0.4
    ident = ({"type": "IAMUser", "principalId": "AIDA" + uuid.UUID(int=r.getrandbits(128)).hex[:16].upper(),
              "arn": f"arn:aws:iam::{account}:user/{u[0]}", "accountId": account, "userName": u[0]}
             if iam_user else
             {"type": "AssumedRole", "principalId": "AROA" + uuid.UUID(int=r.getrandbits(128)).hex[:16].upper() + ":" + u[0],
              "arn": f"arn:aws:sts::{account}:assumed-role/{r.choice(['DeveloperAccess', 'ReadOnly', 'Admin'])}/{u[0]}",
              "accountId": account})
    ev = {
        "eventVersion": "1.09",
        "userIdentity": ident,
        "eventTime": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "eventSource": source,
        "eventName": name,
        "awsRegion": r.choice(REGIONS),
        "sourceIPAddress": r.choice(PUBLIC) if r.random() < 0.8 else f"{source}",
        "userAgent": r.choice(["aws-cli/2.17.40 Python/3.11.9 Linux/6.1", "console.amazonaws.com",
                               "Boto3/1.35.10 Python/3.12.5", "terraform-provider-aws/5.66.0"]),
        "requestID": str(uuid.UUID(int=r.getrandbits(128))),
        "eventID": str(uuid.UUID(int=r.getrandbits(128))),
        "readOnly": name.startswith(("Describe", "Get")) or name == "Decrypt",
        "eventType": etype,
        "managementEvent": True,
        "recipientAccountId": account,
        "eventCategory": "Management",
    }
    if name == "ConsoleLogin":
        ok = r.random() < 0.85
        ev["responseElements"] = {"ConsoleLogin": "Success" if ok else "Failure"}
        ev["additionalEventData"] = {"MFAUsed": "Yes" if r.random() < 0.8 else "No"}
        if not ok:
            ev["errorMessage"] = "Failed authentication"
    elif r.random() < 0.07:
        ev["errorCode"] = "AccessDenied"
        ev["errorMessage"] = f"User is not authorized to perform: {source.split('.')[0]}:{name}"
    return json.dumps(ev, separators=(",", ":"))


def wave(t, period, depth=0.25, phase=0.0):
    """A slow rate modulation, so charts move."""
    return 1.0 + depth * math.sin(2 * math.pi * t / period + phase)
