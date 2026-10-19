/** A source or destination kind: `type` is its bundle type, `category` its group, `href` its own page. A source's
 * group is how a worker group shares the reading of it, as the GUI's add dialog first asks; a destination's is the
 * heading the add dialog lists it under. Each list is in order of `name`. */
export type Connection = {
  id: string;
  type: string;
  name: string;
  summary: string;
  category: string;
  href: string;
};

/** A group of sources: `id` is its heading's anchor on the sources index, `help` what adding workers does for it.
 * In the order the GUI's add dialog offers them (`gui-frontend/src/sourceKinds.ts`). */
export type SourceGroup = { category: string; id: string; help: string };

export const sourceGroups: SourceGroup[] = [
  {
    category: 'Pull, all workers',
    id: 'pull-all-workers',
    help: 'Every worker of the group reads it, and the queue or stream splits the work between them. More workers read more.',
  },
  {
    category: 'Pull, one worker',
    id: 'pull-one-worker',
    help: 'One worker of the group reads it at a time, and the leader keeps its progress. If that worker goes silent another takes over. More workers do not read it faster.',
  },
  {
    category: 'Push, all workers',
    id: 'push-all-workers',
    help: 'Senders connect to it, and every worker of the group listens. More workers take more.',
  },
  {
    category: 'Local, each worker',
    id: 'local-each-worker',
    help: 'Each worker reads what is on its own host.',
  },
];

/** The kinds under each category, the categories in the order of `order`, or of name without it. Kinds with no
 * category, such as steps, make one group with no heading. */
export function byCategory<K extends { category?: string }>(
  kinds: K[],
  order?: string[],
): { category: K['category']; kinds: K[] }[] {
  const categories = [...new Set(kinds.map((k) => k.category))];
  categories.sort(order && ((a, b) => order.indexOf(a ?? '') - order.indexOf(b ?? '')));
  return categories.map((category) => ({
    category,
    kinds: kinds.filter((k) => k.category === category),
  }));
}

export const sources: Connection[] = [
  {
    id: 's3List',
    type: 's3List',
    name: 'Amazon S3',
    summary: 'Read the objects under a prefix of a bucket that cannot notify.',
    category: 'Pull, one worker',
    href: '/docs/sources/s3-list',
  },
  {
    id: 's3Sqs',
    type: 's3Sqs',
    name: 'Amazon S3 via SQS',
    summary: 'Read the objects S3 announces on an SQS queue.',
    category: 'Pull, all workers',
    href: '/docs/sources/s3-sqs',
  },
  {
    id: 'azureBlobList',
    type: 'azureBlobList',
    name: 'Azure Blob Storage',
    summary: 'Read the blobs under a prefix of a container that cannot notify.',
    category: 'Pull, one worker',
    href: '/docs/sources/azure-blob-list',
  },
  {
    id: 'azureBlobQueue',
    type: 'azureBlobQueue',
    name: 'Azure Blob via Storage queue',
    summary: 'Read the blobs Event Grid announces on a Storage queue.',
    category: 'Pull, all workers',
    href: '/docs/sources/azure-blob-queue',
  },
  {
    id: 'eventHubs',
    type: 'eventHubs',
    name: 'Azure Event Hubs',
    summary: 'Read event hubs through their Kafka endpoint.',
    category: 'Pull, all workers',
    href: '/docs/sources/event-hubs',
  },
  {
    id: 'gcsList',
    type: 'gcsList',
    name: 'Google Cloud Storage',
    summary: 'Read the objects under a prefix of a bucket that cannot notify.',
    category: 'Pull, one worker',
    href: '/docs/sources/gcs-list',
  },
  {
    id: 'gcsPubSub',
    type: 'gcsPubSub',
    name: 'Google Cloud Storage via Pub/Sub',
    summary: 'Read the objects Cloud Storage announces on a Pub/Sub subscription.',
    category: 'Pull, all workers',
    href: '/docs/sources/gcs-pubsub',
  },
  {
    id: 'httpApi',
    type: 'httpApi',
    name: 'HTTP API',
    summary: 'Poll an address that answers a GET with JSON or lines of text.',
    category: 'Pull, one worker',
    href: '/docs/sources/http-api',
  },
  {
    id: 'httpReceiver',
    type: 'httpReceiver',
    name: 'HTTP receiver',
    summary: 'Take what senders POST to a path on a port every worker listens on.',
    category: 'Push, all workers',
    href: '/docs/sources/http-receiver',
  },
  {
    id: 'hecReceiver',
    type: 'hecReceiver',
    name: 'HEC receiver',
    summary:
      'Take what Splunk HTTP Event Collector clients send to a port every worker listens on.',
    category: 'Push, all workers',
    href: '/docs/sources/hec-receiver',
  },
  {
    id: 'kafka',
    type: 'kafka',
    name: 'Kafka',
    summary: 'Read topic streams or a range of them.',
    category: 'Pull, all workers',
    href: '/docs/sources/kafka',
  },
  {
    id: 'oktaLogs',
    type: 'oktaLogs',
    name: 'Okta System Log',
    summary: 'Read an Okta org’s System Log, as an API token or a service app.',
    category: 'Pull, one worker',
    href: '/docs/sources/okta-logs',
  },
  {
    id: 'postgres',
    type: 'postgres',
    name: 'PostgreSQL',
    summary: 'Run a query against a PostgreSQL database every poll and read its rows.',
    category: 'Pull, one worker',
    href: '/docs/sources/postgres',
  },
  {
    id: 'mysql',
    type: 'mysql',
    name: 'MySQL and MariaDB',
    summary: 'Run a query against a MySQL or MariaDB database every poll and read its rows.',
    category: 'Pull, one worker',
    href: '/docs/sources/mysql',
  },
  {
    id: 's2sReceiver',
    type: 's2sReceiver',
    name: 'S2S receiver',
    summary: 'Take what Splunk forwarders send over S2S to a port every worker listens on.',
    category: 'Push, all workers',
    href: '/docs/sources/s2s-receiver',
  },
  {
    id: 'otlpReceiver',
    type: 'otlpReceiver',
    name: 'OTLP receiver',
    summary:
      'Take the logs OpenTelemetry exporters send over OTLP/HTTP to a port every worker listens on.',
    category: 'Push, all workers',
    href: '/docs/sources/otlp-receiver',
  },
  {
    id: 'syslogReceiver',
    type: 'syslogReceiver',
    name: 'Syslog receiver',
    summary: 'Take what syslog senders send over TCP or TLS to a port every worker listens on.',
    category: 'Push, all workers',
    href: '/docs/sources/syslog-receiver',
  },
  {
    id: 'prometheusReceiver',
    type: 'prometheusReceiver',
    name: 'Prometheus receiver',
    summary:
      'Take the samples Prometheus sends over remote_write to a port every worker listens on.',
    category: 'Push, all workers',
    href: '/docs/sources/prometheus-receiver',
  },
  {
    id: 'elasticReceiver',
    type: 'elasticReceiver',
    name: 'Elasticsearch receiver',
    summary:
      'Take what Beats, Fluent Bit and other Elasticsearch clients send to a port every worker listens on.',
    category: 'Push, all workers',
    href: '/docs/sources/elastic-receiver',
  },
  {
    id: 'sftp',
    type: 'sftp',
    name: 'SFTP',
    summary: 'Read the files under a directory on an SFTP server.',
    category: 'Pull, one worker',
    href: '/docs/sources/sftp',
  },
  {
    id: 'logs',
    type: 'logs',
    name: 'Smeltr logs',
    summary: 'Collect each worker’s own log records.',
    category: 'Local, each worker',
    href: '/docs/sources/logs',
  },
  {
    id: 'metrics',
    type: 'metrics',
    name: 'Smeltr metrics',
    summary: 'Collect each worker’s own metrics.',
    category: 'Local, each worker',
    href: '/docs/sources/metrics',
  },
];
export const destinations: Connection[] = [
  {
    id: 's3',
    type: 's3',
    name: 'Amazon S3',
    summary: 'Write batches as objects in a bucket.',
    category: 'Object storage',
    href: '/docs/destinations/s3',
  },
  {
    id: 'iceberg',
    type: 'iceberg',
    name: 'Apache Iceberg',
    summary: 'Commit Parquet files to a table in S3 through its REST catalog.',
    category: 'Tables',
    href: '/docs/destinations/iceberg',
  },
  {
    id: 'delta',
    type: 'delta',
    name: 'Delta Lake',
    summary: 'Commit Parquet files to a table in S3 through its log.',
    category: 'Tables',
    href: '/docs/destinations/delta',
  },
  {
    id: 'azureBlob',
    type: 'azureBlob',
    name: 'Azure Blob Storage',
    summary: 'Write batches as blobs in a container.',
    category: 'Object storage',
    href: '/docs/destinations/azure-blob',
  },
  {
    id: 'eventHubs',
    type: 'eventHubs',
    name: 'Azure Event Hubs',
    summary: 'Write records to event hubs through their Kafka endpoint.',
    category: 'Streams',
    href: '/docs/destinations/event-hubs',
  },
  {
    id: 'elasticsearch',
    type: 'elasticsearch',
    name: 'Elasticsearch',
    summary: 'Index documents through the Bulk API.',
    category: 'Log analytics',
    href: '/docs/destinations/elasticsearch',
  },
  {
    id: 'logAnalytics',
    type: 'logAnalytics',
    name: 'Log Analytics workspace',
    summary: 'Send records to a Log Analytics table through a data collection rule.',
    category: 'Log analytics',
    href: '/docs/destinations/log-analytics',
  },
  {
    id: 'gcs',
    type: 'gcs',
    name: 'Google Cloud Storage',
    summary: 'Write batches as objects in a bucket.',
    category: 'Object storage',
    href: '/docs/destinations/gcs',
  },
  {
    id: 'http',
    type: 'http',
    name: 'HTTP',
    summary: 'POST records to any HTTP service, batched or one per request.',
    category: 'Web',
    href: '/docs/destinations/http',
  },
  {
    id: 'kafka',
    type: 'kafka',
    name: 'Kafka',
    summary: 'Write processed records to topics.',
    category: 'Streams',
    href: '/docs/destinations/kafka',
  },
  {
    id: 'prometheus',
    type: 'prometheus',
    name: 'Prometheus remote_write',
    summary: 'Send samples over remote_write to Prometheus or any store that takes it.',
    category: 'Observability',
    href: '/docs/destinations/prometheus',
  },
  {
    id: 'sftp',
    type: 'sftp',
    name: 'SFTP',
    summary: 'Write batches as files in a directory on an SFTP server.',
    category: 'Object storage',
    href: '/docs/destinations/sftp',
  },
  {
    id: 'splunk',
    type: 'splunkHec',
    name: 'Splunk HEC',
    summary: 'Send events to HTTP Event Collector.',
    category: 'Log analytics',
    href: '/docs/destinations/splunk',
  },
];
// Promote entries only when the connector is integrated and its page is ready.
export const upcomingDestinations: Pick<Connection, 'id' | 'name' | 'summary'>[] = [];
