import type { SourceDto } from './api';
import type { CatalogKind } from './TypeCatalog.svelte';

/** How a group shares the reading of a source: what adding workers does for it. */
export type SourceKind = 'pullAll' | 'pullOne' | 'push' | 'local';

const SOURCE = 'M4 22 H18 V42 H4 Z';
const dot = (cx: number, cy: number, r: number) =>
  `M${cx - r} ${cy} A${r} ${r} 0 1 0 ${cx + r} ${cy} A${r} ${r} 0 1 0 ${cx - r} ${cy} Z`;

/** In the order the add dialog offers them. */
export const SOURCE_KINDS: CatalogKind<SourceKind>[] = [
  {
    kind: 'pullAll',
    label: 'Pull, all workers',
    help: 'Every worker of the group reads it, and the queue or stream splits the work between them. More workers read more.',
    // One source fanning out to three workers.
    glyph: {
      d: `${SOURCE} M18 30 H47 V34 H18 Z M18 30 L45 12 L47 15 L20 33 Z M20 31 L47 49 L45 52 L18 34 Z ${dot(53, 13, 6)} ${dot(53, 32, 6)} ${dot(53, 51, 6)}`,
    },
  },
  {
    kind: 'pullOne',
    label: 'Pull, one worker',
    help: 'One worker of the group reads it at a time, and the leader keeps its progress. If that worker goes silent another takes over. More workers do not read it faster.',
    // One source, one worker.
    glyph: { d: `${SOURCE} M18 30 H46 V34 H18 Z ${dot(53, 32, 8)}` },
  },
  {
    kind: 'push',
    label: 'Push, all workers',
    help: 'Senders connect to it, and every worker of the group listens. More workers take more.',
    // Three senders converging on one source.
    glyph: {
      d: `M46 22 H60 V42 H46 Z M17 30 H46 V34 H17 Z M46 30 L19 12 L17 15 L44 33 Z M44 31 L17 49 L19 52 L46 34 Z ${dot(11, 13, 6)} ${dot(11, 32, 6)} ${dot(11, 51, 6)}`,
    },
  },
  {
    kind: 'local',
    label: 'Local, each worker',
    help: 'Each worker reads what is on its own host.',
    // A host's screen on its stand.
    glyph: {
      d: 'M4 8 H60 V44 H4 Z M10 14 V38 H54 V14 Z M28 44 H36 V52 H28 Z M18 52 H46 V58 H18 Z',
      evenodd: true,
    },
  },
];

/** The kind of every source type. */
export const SOURCE_KIND: Record<SourceDto['type'], SourceKind> = {
  kafka: 'pullAll',
  eventHubs: 'pullAll',
  s3Sqs: 'pullAll',
  azureBlobQueue: 'pullAll',
  gcsPubSub: 'pullAll',
  s3List: 'pullOne',
  gcsList: 'pullOne',
  azureBlobList: 'pullOne',
  sftp: 'pullOne',
  httpApi: 'pullOne',
  oktaLogs: 'pullOne',
  postgres: 'pullOne',
  mysql: 'pullOne',
  httpReceiver: 'push',
  hecReceiver: 'push',
  s2sReceiver: 'push',
  otlpReceiver: 'push',
  syslogReceiver: 'push',
  prometheusReceiver: 'push',
  elasticReceiver: 'push',
  metrics: 'local',
  logs: 'local',
};
