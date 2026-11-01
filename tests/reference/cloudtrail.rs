//! AWS CloudTrail: one record is one delivery, `{"Records": [...]}`, and
//! every event in it becomes one ECS document.
//!
//! The pipeline is the shape `docs/examples.md` section 6 gives: a JSON
//! parse that types the fields the document reads, an unroll that makes
//! one row per event, and the mapping. What the service sent besides --
//! request and response objects, session context, TLS details -- rides
//! through untouched under `aws.cloudtrail`.

use serde_json::{Value, json};

use super::{Clock, Pipeline, mix, pick};

pub const PIPELINE: Pipeline = Pipeline {
    name: "cloudtrail",
    line,
    bundle,
    check,
};

// -- the record ---------------------------------------------------------------

/// A delivery file is per account and per region, so both are fixed for a
/// record and vary across the corpus.
const ACCOUNTS: &[&str] = &[
    "123456789012",
    "210987654321",
    "334455667788",
    "498712340056",
    "602938475610",
    "771234509876",
];

/// Weighted the way a real organization's trail is: most traffic in the
/// home region.
const REGIONS: &[&str] = &[
    "us-east-1",
    "us-east-1",
    "us-east-1",
    "us-east-1",
    "us-west-2",
    "us-west-2",
    "eu-west-1",
    "eu-central-1",
    "ap-southeast-2",
    "ap-northeast-1",
];

const USERS: &[&str] = &[
    "alice",
    "bob",
    "carol",
    "ci-deployer",
    "terraform",
    "backup-svc",
    "dmitri",
    "priya",
];

const ROLES: &[&str] = &[
    "deploy",
    "ReadOnly",
    "AWSReservedSSO_AdministratorAccess_4c1e0a9b7d2f3e18",
    "AWSReservedSSO_PowerUserAccess_9a2b7c6d5e4f3a21",
    "ecsTaskExecutionRole",
    "lambda-orders-exec",
    "eks-node-group-role",
    "github-actions-oidc",
];

const AGENTS: &[&str] = &[
    "aws-cli/2.15.30 Python/3.11.8 Linux/6.1.79-99.164.amzn2023.x86_64 exe/x86_64.amzn.2023 prompt/off command/s3.cp",
    "Boto3/1.34.69 md/Botocore#1.34.69 ua/2.0 os/linux#5.10.213-201.855.amzn2.x86_64 md/arch#x86_64 lang/python#3.12.2 \
     md/pyimpl#CPython cfg/retry-mode#legacy Botocore/1.34.69",
    "aws-sdk-go-v2/1.25.3 os/linux lang/go#1.22.1 md/GOOS#linux md/GOARCH#amd64 api/s3#1.51.4",
    "APN/1.0 HashiCorp/1.0 Terraform/1.7.5 (+https://www.terraform.io) terraform-provider-aws/5.42.0 \
     (+https://registry.terraform.io/providers/hashicorp/aws) aws-sdk-go-v2/1.25.3 os/linux lang/go#1.22.1",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 \
     Safari/537.36",
    "aws-sdk-java/2.25.16 Linux/5.10.213-201.855.amzn2.x86_64 OpenJDK_64-Bit_Server_VM/17.0.10+7-LTS Java/17.0.10 \
     vendor/Amazon.com_Inc. io/sync http/Apache cfg/retry-mode/legacy",
];

const BUCKETS: &[&str] = &[
    "acme-prod-artifacts",
    "acme-logs-archive",
    "acme-data-lake-raw",
    "acme-tfstate",
];

/// `prefix` and 16 characters of an access-key-shaped id, from `h`.
fn key_id(prefix: &str, h: u64) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
    let mut s = String::from(prefix);
    let mut x = h;
    for _ in 0..16 {
        s.push(ALPHABET[(x % 32) as usize] as char);
        x = (x / 32) ^ mix(x);
    }
    s
}

fn uuid(h: u64) -> String {
    let g = mix(h);
    format!(
        "{:08x}-{:04x}-4{:03x}-{:04x}-{:012x}",
        h >> 32,
        h & 0xffff,
        (g >> 4) & 0xfff,
        0x8000 | (g >> 20) & 0x3fff,
        g >> 16
    )
}

/// How many events delivery `i` carries: 1 to 20, mean about five, with the
/// long tail a busy minute gives. Most deliveries are small.
fn event_count(i: u64) -> u64 {
    let h = mix(i ^ 0xC10D_7EA1);
    // Two small draws give the body, 1 to 7; one delivery in eight carries
    // a burst of up to 13 more.
    let burst = if (h >> 24).is_multiple_of(8) { (h >> 16) % 14 } else { 0 };
    1 + h % 4 + (h >> 8) % 4 + burst
}

/// Who made the call. Assumed roles dominate a modern account; long-lived
/// IAM users, service principals and the occasional root sign-in fill the
/// rest.
fn identity(acct: &str, h: u64, at: &Clock) -> Value {
    match h % 100 {
        0..=59 => {
            let role = pick(ROLES, h >> 8);
            let session = format!("{}-{}", pick(USERS, h >> 16), 1_700_000_000 + (h >> 20) % 10_000_000);
            let role_pid = key_id("AROA", h >> 4);
            json!({
                "type": "AssumedRole",
                "principalId": format!("{role_pid}:{session}"),
                "arn": format!("arn:aws:sts::{acct}:assumed-role/{role}/{session}"),
                "accountId": acct,
                "accessKeyId": key_id("ASIA", h >> 12),
                "sessionContext": {
                    "sessionIssuer": {
                        "type": "Role", "principalId": role_pid,
                        "arn": format!("arn:aws:iam::{acct}:role/{role}"),
                        "accountId": acct, "userName": role,
                    },
                    "webIdFederationData": {},
                    "attributes": {
                        "creationDate": at.plus(-(((h >> 24) % 3_600) as i64)).iso(),
                        "mfaAuthenticated": if (h >> 30).is_multiple_of(3) { "true" } else { "false" },
                    },
                },
            })
        }
        60..=79 => {
            let user = pick(USERS, h >> 8);
            json!({
                "type": "IAMUser",
                "principalId": key_id("AIDA", h >> 4),
                "arn": format!("arn:aws:iam::{acct}:user/{user}"),
                "accountId": acct,
                "accessKeyId": key_id("AKIA", h >> 12),
                "userName": user,
            })
        }
        80..=97 => json!({
            "type": "AWSService",
            "invokedBy": pick(&["ec2.amazonaws.com", "lambda.amazonaws.com", "cloudformation.amazonaws.com",
                                "autoscaling.amazonaws.com", "config.amazonaws.com"], h >> 8),
        }),
        _ => json!({
            "type": "Root",
            "principalId": acct,
            "arn": format!("arn:aws:iam::{acct}:root"),
            "accountId": acct,
            "accessKeyId": "",
        }),
    }
}

/// Event `k` of delivery `i`, as CloudTrail writes it, the delivery made at
/// `at`.
pub fn event(i: u64, k: u64, at: &Clock) -> Value {
    let r = mix(i);
    let acct = pick(ACCOUNTS, r);
    let region = pick(REGIONS, r >> 8);
    let h = mix(i.wrapping_mul(64).wrapping_add(k) ^ 0x5EF6);
    let g = mix(h);
    let who = identity(acct, g, at);
    let service = who["type"] == "AWSService";
    let bucket = pick(BUCKETS, h >> 20);
    let object = format!(
        "{}/{}/{:02}/{:02}/part-{:05}.parquet",
        pick(&["orders", "events", "metrics"], h >> 24),
        at.year,
        at.month,
        at.day,
        (h >> 32) % 100_000
    );
    let instance = format!("i-0{:016x}", g >> 8);

    // (source, name, readOnly, data event, request, response, the errors this call is seen to fail with)
    let (source, name, read_only, data, request, response, errors): (&str, &str, bool, bool, Value, Value, &[&str]) =
        match h % 100 {
            0..=21 => (
                "s3.amazonaws.com",
                "GetObject",
                true,
                true,
                json!({"bucketName": bucket, "Host": format!("{bucket}.s3.{region}.amazonaws.com"), "key": object}),
                Value::Null,
                &["AccessDenied", "NoSuchKey"],
            ),
            22..=31 => (
                "s3.amazonaws.com",
                "PutObject",
                false,
                true,
                json!({"bucketName": bucket, "Host": format!("{bucket}.s3.{region}.amazonaws.com"), "key": object,
                       "x-amz-server-side-encryption": "aws:kms"}),
                json!({"x-amz-server-side-encryption": "aws:kms",
                       "x-amz-server-side-encryption-aws-kms-key-id": format!("arn:aws:kms:{region}:{acct}:key/{}", uuid(g))}),
                &["AccessDenied"],
            ),
            32..=37 => (
                "s3.amazonaws.com",
                "ListObjects",
                true,
                true,
                json!({"bucketName": bucket, "Host": format!("{bucket}.s3.{region}.amazonaws.com"),
                       "prefix": "orders/2026/10/", "list-type": "2", "max-keys": "1000"}),
                Value::Null,
                &["AccessDenied"],
            ),
            38..=47 => (
                "kms.amazonaws.com",
                "Decrypt",
                true,
                false,
                json!({"encryptionAlgorithm": "SYMMETRIC_DEFAULT",
                       "encryptionContext": {"aws:s3:arn": format!("arn:aws:s3:::{bucket}/{object}")}}),
                Value::Null,
                &["AccessDeniedException"],
            ),
            48..=53 => (
                "kms.amazonaws.com",
                "GenerateDataKey",
                true,
                false,
                json!({"keyId": format!("arn:aws:kms:{region}:{acct}:key/{}", uuid(g)), "keySpec": "AES_256"}),
                Value::Null,
                &[],
            ),
            54..=61 => (
                "sts.amazonaws.com",
                "AssumeRole",
                true,
                false,
                json!({"roleArn": format!("arn:aws:iam::{acct}:role/{}", pick(ROLES, g >> 3)),
                       "roleSessionName": format!("{}-{}", pick(USERS, g >> 5), g % 100_000), "durationSeconds": 3600}),
                json!({"credentials": {"accessKeyId": key_id("ASIA", g), "sessionToken": format!("IQoJb3JpZ2luX2VjE{:x}{:x}", g, h),
                       "expiration": "Oct 2, 2026, 1:12:03 AM"},
                       "assumedRoleUser": {"assumedRoleId": format!("{}:{}", key_id("AROA", g >> 2), pick(USERS, g >> 5)),
                       "arn": format!("arn:aws:sts::{acct}:assumed-role/{}/{}", pick(ROLES, g >> 3), pick(USERS, g >> 5))}}),
                &["AccessDenied"],
            ),
            62..=65 => (
                "sts.amazonaws.com",
                "GetCallerIdentity",
                true,
                false,
                Value::Null,
                Value::Null,
                &[],
            ),
            66..=73 => (
                "ec2.amazonaws.com",
                "DescribeInstances",
                true,
                false,
                json!({"instancesSet": {}, "filterSet": {"items": [{"name": "instance-state-name", "valueSet": {"items": [{"value": "running"}]}}]}}),
                Value::Null,
                &["Client.UnauthorizedOperation", "Client.RequestLimitExceeded"],
            ),
            74..=76 => (
                "ec2.amazonaws.com",
                "RunInstances",
                false,
                false,
                json!({"instancesSet": {"items": [{"imageId": format!("ami-0{:016x}", g >> 4), "minCount": 1, "maxCount": 1}]},
                       "instanceType": pick(&["t3.medium", "m6i.large", "c7g.xlarge", "r6i.2xlarge"], g >> 6),
                       "subnetId": format!("subnet-0{:016x}", g >> 12), "monitoring": {"enabled": false}}),
                json!({"requestId": uuid(h), "reservationId": format!("r-0{:016x}", g >> 2), "ownerId": acct,
                       "instancesSet": {"items": [{"instanceId": instance, "instanceState": {"code": 0, "name": "pending"},
                       "privateIpAddress": format!("10.{}.{}.{}", (g >> 8) % 64, (g >> 16) % 256, (g >> 24) % 254 + 1)}]}}),
                &["Client.UnauthorizedOperation", "Server.InsufficientInstanceCapacity"],
            ),
            77..=78 => (
                "ec2.amazonaws.com",
                "AuthorizeSecurityGroupIngress",
                false,
                false,
                json!({"groupId": format!("sg-0{:016x}", g >> 4), "ipPermissions": {"items": [{"ipProtocol": "tcp",
                       "fromPort": 443, "toPort": 443, "ipRanges": {"items": [{"cidrIp": "0.0.0.0/0"}]}}]}}),
                json!({"requestId": uuid(h), "_return": true}),
                &["Client.InvalidPermission.Duplicate"],
            ),
            79..=83 => (
                "iam.amazonaws.com",
                "GetRole",
                true,
                false,
                json!({"roleName": pick(ROLES, g >> 3)}),
                Value::Null,
                &["NoSuchEntityException"],
            ),
            84..=85 => (
                "iam.amazonaws.com",
                "CreateAccessKey",
                false,
                false,
                json!({"userName": pick(USERS, g >> 3)}),
                json!({"accessKey": {"userName": pick(USERS, g >> 3), "accessKeyId": key_id("AKIA", g), "status": "Active",
                       "createDate": "Oct 1, 2026, 12:12:03 AM"}}),
                &["LimitExceededException", "AccessDenied"],
            ),
            86..=88 => (
                "secretsmanager.amazonaws.com",
                "GetSecretValue",
                true,
                false,
                json!({"secretId": format!("arn:aws:secretsmanager:{region}:{acct}:secret:prod/orders/db-{:06x}", g % 0xffffff)}),
                Value::Null,
                &["AccessDeniedException", "ResourceNotFoundException"],
            ),
            89..=91 => (
                "logs.amazonaws.com",
                "CreateLogStream",
                false,
                false,
                json!({"logGroupName": "/aws/lambda/orders-handler", "logStreamName": format!("2026/10/01/[$LATEST]{:032x}", g as u128 * 7)}),
                Value::Null,
                &["ResourceAlreadyExistsException"],
            ),
            92..=94 => (
                "lambda.amazonaws.com",
                "UpdateFunctionCode20150331v2",
                false,
                false,
                json!({"functionName": "orders-handler", "s3Bucket": bucket, "s3Key": "builds/orders-handler.zip", "publish": true}),
                json!({"functionName": "orders-handler", "functionArn": format!("arn:aws:lambda:{region}:{acct}:function:orders-handler"),
                       "runtime": "python3.12", "codeSize": 4_182_331 + g % 10_000, "version": "$LATEST", "state": "Pending"}),
                &["ResourceConflictException"],
            ),
            95..=97 => (
                "dynamodb.amazonaws.com",
                "DescribeTable",
                true,
                false,
                json!({"tableName": pick(&["orders", "sessions", "inventory"], g >> 3)}),
                Value::Null,
                &["ResourceNotFoundException"],
            ),
            _ => (
                "signin.amazonaws.com",
                "ConsoleLogin",
                false,
                false,
                Value::Null,
                json!({"ConsoleLogin": "Success"}),
                &[],
            ),
        };

    // About one call in twelve fails; most failures are a policy saying no.
    let error = if !errors.is_empty() && (g >> 40).is_multiple_of(12) {
        Some(errors[((g >> 44) % errors.len() as u64) as usize])
    } else {
        None
    };
    let console = name == "ConsoleLogin";
    // A console sign-in is a person's, never a service principal's.
    let who = if console && service {
        identity(acct, 60 + g % 20, at)
    } else {
        who
    };
    let service = who["type"] == "AWSService";
    let ip = if service {
        who["invokedBy"].as_str().unwrap().to_string()
    } else {
        format!(
            "{}.{}",
            pick(&["203.0.113", "198.51.100", "192.0.2", "100.24.16"], g >> 48),
            (g >> 50) % 254 + 1
        )
    };
    let agent = if service {
        who["invokedBy"].as_str().unwrap().to_string()
    } else if console {
        AGENTS[4].to_string()
    } else {
        pick(AGENTS, g >> 52).to_string()
    };

    let mut e = json!({
        "eventVersion": if data { "1.09" } else { "1.08" },
        "userIdentity": who,
        // A delivery holds the last few minutes of calls.
        "eventTime": at.plus(-(((k * 13) % 300) as i64)).iso(),
        "eventSource": source,
        "eventName": name,
        "awsRegion": region,
        "sourceIPAddress": ip,
        "userAgent": agent,
        "requestParameters": request,
        "responseElements": if error.is_some() { Value::Null } else { response },
        "requestID": if source == "s3.amazonaws.com" { key_id("", h) } else { uuid(h ^ 1) },
        "eventID": uuid(h),
        "readOnly": read_only,
        "eventType": if console { "AwsConsoleSignIn" } else { "AwsApiCall" },
        "managementEvent": !data,
        "recipientAccountId": acct,
        "eventCategory": if data { "Data" } else { "Management" },
    });
    let o = e.as_object_mut().unwrap();
    if let Some(code) = error {
        o.insert("errorCode".into(), json!(code));
        o.insert(
            "errorMessage".into(),
            json!(format!(
                "User: {} is not authorized to perform: {}:{}",
                o["userIdentity"]["arn"].as_str().unwrap_or("unknown"),
                source.trim_end_matches(".amazonaws.com"),
                name
            )),
        );
    }
    if data {
        o.insert(
            "resources".into(),
            json!([{"type": "AWS::S3::Object", "ARN": format!("arn:aws:s3:::{bucket}/{object}")},
                   {"accountId": acct, "type": "AWS::S3::Bucket", "ARN": format!("arn:aws:s3:::{bucket}")}]),
        );
        o.insert(
            "additionalEventData".into(),
            json!({"SignatureVersion": "SigV4", "CipherSuite": "TLS_AES_128_GCM_SHA256", "bytesTransferredIn": 0,
                   "bytesTransferredOut": (g >> 8) % 4_000_000, "AuthenticationMethod": "AuthHeader",
                   "x-amz-id-2": format!("{:x}{:x}", g, h)}),
        );
    } else if console {
        o.insert(
            "additionalEventData".into(),
            json!({"LoginTo": "https://console.aws.amazon.com/console/home", "MobileVersion": "No",
                   "MFAUsed": if g.is_multiple_of(4) { "No" } else { "Yes" }}),
        );
    }
    if !service {
        o.insert(
            "tlsDetails".into(),
            json!({"tlsVersion": "TLSv1.3", "cipherSuite": "TLS_AES_128_GCM_SHA256",
                   "clientProvidedHostHeader": format!("{}.{region}.amazonaws.com", source.trim_end_matches(".amazonaws.com"))}),
        );
    }
    e
}

/// Delivery `i`: the file CloudTrail writes to S3, one per account, region
/// and few minutes.
pub fn line(i: u64) -> String {
    record(i, &Clock::of(i))
}

/// Delivery `i`, written at `at`.
pub fn record(i: u64, at: &Clock) -> String {
    let records: Vec<Value> = (0..event_count(i)).map(|k| event(i, k, at)).collect();
    json!({ "Records": records }).to_string()
}

// -- the pipeline -------------------------------------------------------------

/// The element's fields the document reads, declared once for every event.
/// The struct is open, so the rest of each event rides along unclaimed.
/// `eventTime` is text here: a datetime is declared only at a parser's top
/// level, so the mapping converts it.
fn parse_step() -> Value {
    let s = |n: &str| json!({"name": n, "type": "str"});
    json!({
        "id": "delivery", "type": "parser", "input": "_raw", "kind": {"type": "json"},
        "declared": [{
            "name": "Records",
            "type": {"type": "array", "of": {"type": "struct", "open": true, "fields": [
                s("eventTime"), s("eventName"), s("eventSource"), s("awsRegion"),
                s("sourceIPAddress"), s("recipientAccountId"), s("errorCode"),
                {"name": "userIdentity", "type": {"type": "struct", "open": true, "fields": [s("type"), s("arn")]}},
            ]}},
        }],
    })
}

/// The mapping: derived fields by `eval`, the declared ones taken out of
/// the event, and what is left moved whole under `aws.cloudtrail`.
fn steps() -> Vec<Value> {
    let evals = [
        ("@timestamp", "parseTime(Records.eventTime)"),
        ("event.action", "Records.eventName"),
        ("event.provider", "Records.eventSource"),
        ("event.outcome", r#"Records.errorCode == null ? "success" : "failure""#),
        ("user.id", "Records.userIdentity.arn"),
        ("user.type", "Records.userIdentity.type"),
        ("source.ip", "Records.sourceIPAddress"),
        ("cloud.provider", r#""aws""#),
        ("cloud.region", "Records.awsRegion"),
        ("cloud.account.id", "Records.recipientAccountId"),
    ];
    // Declared, so not part of what passes through.
    let moved = [
        "eventTime",
        "eventName",
        "eventSource",
        "awsRegion",
        "sourceIPAddress",
        "recipientAccountId",
        "userIdentity",
    ];
    vec![
        parse_step(),
        // Dropping the parent fields: each delivery's other keys would
        // otherwise be copied onto every one of its events.
        json!({"id": "events", "type": "unroll", "field": "Records", "keepParentFields": false}),
        json!({
            "id": "derive", "type": "eval",
            "assignments": evals.iter().map(|(t, e)| json!({"target": t, "expr": e})).collect::<Vec<_>>(),
        }),
        json!({
            "id": "prune", "type": "eval",
            "assignments": moved.iter().map(|n| json!({"target": format!("Records.{n}"), "expr": "null"})).collect::<Vec<_>>(),
        }),
        json!({
            "id": "to-ecs", "type": "rename",
            "pairs": [
                {"from": "Records.errorCode", "to": "Records.error_code"},
                {"from": "Records", "to": "aws.cloudtrail"},
            ],
        }),
    ]
}

fn bundle() -> String {
    super::bundle(steps(), vec![])
}

// -- the check ----------------------------------------------------------------

fn check(rows: u64, docs: u64, kept: &[Value]) {
    // `run` starts counting after a warm-up batch, and `check` is not told
    // where the counted window starts: find the window whose event total is
    // `docs`. A pipeline that did not unroll gives `docs == rows`, which no
    // window matches.
    let sums: Vec<u64> = (0..2 * rows).map(event_count).collect();
    let found = (0..=rows as usize).any(|w| sums[w..w + rows as usize].iter().sum::<u64>() == docs);
    assert!(
        docs > rows,
        "one document per event, not per delivery: {docs} docs from {rows} rows"
    );
    assert!(found, "{docs} documents is no window of {rows} deliveries' events");

    // The kept documents are the first events of the corpus, in order.
    // Its seed puts a failed call and every kind of caller among them, a
    // service principal's missing ARN included.
    let outcomes: Vec<&Value> = kept.iter().map(|d| &d["event"]["outcome"]).collect();
    assert!(outcomes.contains(&&json!("failure")), "a failure among {outcomes:?}");
    assert!(
        kept.iter()
            .any(|d| d["user"]["type"] == "AWSService" && d["user"].get("id").is_none())
    );
    let expected = (0..).flat_map(|i| (0..event_count(i)).map(move |k| event(i, k, &Clock::of(i))));
    for (doc, e) in kept.iter().zip(expected) {
        let at = |p: &str| doc.pointer(p).unwrap_or(&Value::Null);
        let ts = at("/@timestamp").as_str().expect("@timestamp is a timestamp");
        assert!(
            ts.starts_with(e["eventTime"].as_str().unwrap().trim_end_matches('Z')),
            "@timestamp {ts} from {}",
            e["eventTime"]
        );
        assert_eq!(at("/event/action"), &e["eventName"], "{doc}");
        assert_eq!(at("/event/provider"), &e["eventSource"]);
        let outcome = if e.get("errorCode").is_some() {
            "failure"
        } else {
            "success"
        };
        assert_eq!(at("/event/outcome"), outcome, "{doc}");
        assert_eq!(
            at("/aws/cloudtrail/error_code"),
            e.get("errorCode").unwrap_or(&Value::Null)
        );
        assert_eq!(at("/user/id"), &e["userIdentity"]["arn"]);
        assert_eq!(at("/user/type"), &e["userIdentity"]["type"]);
        assert_eq!(at("/source/ip"), &e["sourceIPAddress"]);
        assert_eq!(at("/cloud/provider"), "aws");
        assert_eq!(at("/cloud/region"), &e["awsRegion"]);
        assert_eq!(at("/cloud/account/id"), &e["recipientAccountId"]);
        // The rest of the event passes through as the service sent it.
        assert_eq!(at("/aws/cloudtrail/eventVersion"), &e["eventVersion"]);
        assert_eq!(at("/aws/cloudtrail/eventID"), &e["eventID"]);
        assert_eq!(
            at("/aws/cloudtrail/requestParameters"),
            &e["requestParameters"],
            "{doc}"
        );
        assert_eq!(at("/aws/cloudtrail/readOnly"), &e["readOnly"]);
        for gone in ["eventName", "userIdentity", "eventTime", "errorCode", "Records"] {
            assert!(doc["aws"]["cloudtrail"].get(gone).is_none(), "{gone} moved: {doc}");
        }
        assert!(doc.get("Records").is_none() && doc.get("_raw").is_none(), "{doc}");
    }
}
