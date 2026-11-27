// The documentation diagrams. Each diagram states its text once, and each layout only places it: the wide layout
// for a page, the compact one for a phone. The types make every layout place every node, label and boundary, so the
// two layouts cannot say different things.

type Point = { x: number; y: number };
type Box = Point & { w: number };
type Area = Box & { h: number };

type Node = {
  title: string;
  detail: string[];
  role?: string;
  /** Drawn as a stack of boxes: there are many of it. */
  stacked?: boolean;
  /** Continues the node just above it, as one box with a divider. */
  joined?: boolean;
};

type Layout<N extends string, L extends string, B extends string> = {
  width: number;
  height: number;
  nodes: Record<N, Box>;
  paths: string[];
  controlPaths?: string[];
  storagePaths?: string[];
} & Places<'labels', L, Point> &
  Places<'boundaries', B, Area>;

/** `labels` or `boundaries`: required when the diagram has any, so no layout leaves one out. */
type Places<K extends string, Id extends string, T> = [Id] extends [never]
  ? { [P in K]?: never }
  : { [P in K]: Record<Id, T> };

type Spec<N extends string, L extends string, B extends string> = {
  title: string;
  description: string;
  caption: string;
  nodes: Record<N, Node>;
  labels?: Record<L, string>;
  boundaries?: Record<B, string>;
  /** The wide layout, then the compact one. */
  layouts: [Layout<NoInfer<N>, NoInfer<L>, NoInfer<B>>, Layout<NoInfer<N>, NoInfer<L>, NoInfer<B>>];
};

/** A diagram, with each layout's positions joined to the text they place. */
function diagram<N extends string, L extends string = never, B extends string = never>(
  spec: Spec<N, L, B>,
) {
  const { nodes, labels = {} as Record<L, string>, boundaries = {} as Record<B, string> } = spec;
  return {
    title: spec.title,
    description: spec.description,
    caption: spec.caption,
    layouts: spec.layouts.map((layout) => ({
      width: layout.width,
      height: layout.height,
      paths: layout.paths,
      controlPaths: layout.controlPaths ?? [],
      storagePaths: layout.storagePaths ?? [],
      nodes: (Object.keys(nodes) as N[]).map((id) => ({ ...nodes[id], ...layout.nodes[id] })),
      labels: (Object.keys(labels) as L[]).map((id) => ({
        text: labels[id],
        ...(layout.labels as Record<L, Point>)[id],
      })),
      boundaries: (Object.keys(boundaries) as B[]).map((id) => ({
        label: boundaries[id],
        ...(layout.boundaries as Record<B, Area>)[id],
      })),
    })),
  };
}

export type Diagram = ReturnType<typeof diagram>;
export type DiagramNode = Diagram['layouts'][number]['nodes'][number];

export const diagrams = {
  review: diagram({
    title: 'From a draft to running workers',
    description:
      'Draft changes go to review. Review can send them back for revision. Approval and deployment publish a commit on main. Each worker fetches main and reports its running commit.',
    caption:
      'Review the diff and test results before approval. Each worker fetches main independently; check its running commit to confirm adoption.',
    nodes: {
      draftBranch: { title: 'Draft branch', detail: ['Save your edits'] },
      review: { title: 'Review', detail: ['Diff and test results'] },
      approveDeploy: { title: 'Approve & deploy', detail: ['Publish to main'] },
      workerA: { title: 'Worker A', detail: ['Fetch main', 'Report commit'] },
      workerB: { title: 'Worker B', detail: ['Fetch main', 'Report commit'] },
    },
    labels: { revise: 'Revise' },
    layouts: [
      {
        width: 900,
        height: 350,
        nodes: {
          draftBranch: { x: 20, y: 50, w: 220 },
          review: { x: 320, y: 50, w: 220 },
          approveDeploy: { x: 620, y: 50, w: 260 },
          workerA: { x: 430, y: 250, w: 210 },
          workerB: { x: 670, y: 250, w: 210 },
        },
        labels: { revise: { x: 280, y: 177 } },
        paths: [
          'M240 90H310',
          'M540 90H610',
          'M430 130V185H130V140',
          'M750 130V205H535V240',
          'M750 205H775V240',
        ],
      },
      {
        width: 360,
        height: 500,
        nodes: {
          draftBranch: { x: 80, y: 10, w: 200 },
          review: { x: 80, y: 130, w: 200 },
          approveDeploy: { x: 80, y: 250, w: 200 },
          workerA: { x: 10, y: 400, w: 160 },
          workerB: { x: 190, y: 400, w: 160 },
        },
        labels: { revise: { x: 61, y: 112 } },
        paths: [
          'M180 90V120',
          'M180 210V240',
          'M80 170H35V50H70',
          'M180 330V370H90V390',
          'M180 370H270V390',
        ],
      },
    ],
  }),
  delivery: diagram({
    title: 'Delivery and recovery',
    description:
      'When a destination accepts a record, that delivery is complete. If it is unreachable, Smeltr retries and pauses input when buffers fill. If it refuses a record, Smeltr follows your chosen action: hold and retry, send it elsewhere for investigation, or count and drop it.',
    caption:
      'Choose any normal Smeltr destination to receive refused records for investigation and later replay. Selecting it for this job makes it a dead-letter destination; it is not a separate destination type. Smeltr waits for every selected destination to confirm receipt. If one is unavailable, the input streams sending records to it can still be paused.',
    nodes: {
      destinationResponse: {
        title: 'Destination response',
        detail: ['What happened to the record?'],
        role: 'destination',
      },
      accepted: { title: 'Accepted', detail: ['This delivery is complete'] },
      unreachable: { title: 'Unreachable', detail: ['Retry; pause when buffers fill'] },
      recordRefused: { title: 'Record refused', detail: ['Use the action you selected'] },
      holdAndRetryDefault: {
        title: 'Hold and retry (default)',
        detail: ['Keep the record; pause input'],
      },
      sendElsewhere: {
        title: 'Send elsewhere',
        detail: ['Wait for each chosen', 'destination to accept'],
      },
      drop: { title: 'Drop', detail: ['Delete the record;', 'count the drops'] },
    },
    layouts: [
      {
        width: 900,
        height: 510,
        nodes: {
          destinationResponse: { x: 320, y: 10, w: 260 },
          accepted: { x: 20, y: 150, w: 260 },
          unreachable: { x: 320, y: 150, w: 260 },
          recordRefused: { x: 620, y: 150, w: 260 },
          holdAndRetryDefault: { x: 20, y: 390, w: 260 },
          sendElsewhere: { x: 320, y: 390, w: 260 },
          drop: { x: 620, y: 390, w: 260 },
        },
        paths: [
          'M450 90V120H150V140',
          'M450 120V140',
          'M450 120H750V140',
          'M750 230V320H150V380',
          'M450 320V380',
          'M750 320V380',
        ],
      },
      {
        width: 360,
        height: 870,
        nodes: {
          destinationResponse: { x: 80, y: 10, w: 260 },
          accepted: { x: 80, y: 145, w: 260 },
          unreachable: { x: 80, y: 280, w: 260 },
          recordRefused: { x: 80, y: 415, w: 260 },
          holdAndRetryDefault: { x: 100, y: 545, w: 240 },
          sendElsewhere: { x: 100, y: 655, w: 240 },
          drop: { x: 100, y: 765, w: 240 },
        },
        paths: [
          'M80 50H25V455H70',
          'M25 185H70',
          'M25 320H70',
          'M210 495V515H60V805H90',
          'M60 585H90',
          'M60 695H90',
        ],
      },
    ],
  }),
  system: diagram({
    title: 'How Smeltr fits together',
    description:
      'An administrator edits and reviews configuration on the leader. The leader stores it in Git. Workers fetch main and report their status to the leader. Source records flow through workers to destinations. The stacked worker cards represent one or more workers.',
    caption:
      'The leader provides the editing and review screens and stores configuration in Git. Workers fetch main and report their status. Workers also connect directly to sources and destinations, where they read, filter, and deliver records. Dashed arrows show workers fetching configuration and reporting status. The stack represents one or more workers.',
    nodes: {
      administrator: {
        title: 'Administrator',
        detail: ['Edit · test · review'],
        role: 'administrator',
      },
      leader: { title: 'Leader', detail: ['UI + Git repository'], role: 'leader' },
      sources: { title: 'Sources', detail: ['Input records'], role: 'source' },
      workers: {
        title: 'Workers',
        detail: ['Filter and deliver records'],
        role: 'worker',
        stacked: true,
      },
      destinations: { title: 'Destinations', detail: ['Kept records'], role: 'destination' },
    },
    labels: { fetchMain: 'Fetch main', reportStatus: 'Report status' },
    boundaries: { smeltr: 'Smeltr' },
    layouts: [
      {
        width: 900,
        height: 400,
        nodes: {
          administrator: { x: 20, y: 60, w: 220 },
          leader: { x: 340, y: 60, w: 220 },
          sources: { x: 20, y: 290, w: 220 },
          workers: { x: 340, y: 290, w: 220 },
          destinations: { x: 660, y: 290, w: 220 },
        },
        labels: { fetchMain: { x: 540, y: 210 }, reportStatus: { x: 540, y: 231 } },
        boundaries: { smeltr: { x: 300, y: 8, w: 312, h: 372 } },
        paths: ['M240 100H330', 'M240 330H330', 'M560 330H650'],
        controlPaths: ['M450 290V150'],
      },
      {
        width: 360,
        height: 690,
        nodes: {
          administrator: { x: 60, y: 10, w: 240 },
          sources: { x: 60, y: 140, w: 240 },
          leader: { x: 60, y: 290, w: 240 },
          workers: { x: 60, y: 450, w: 240 },
          destinations: { x: 60, y: 590, w: 240 },
        },
        labels: { fetchMain: { x: 255, y: 400 }, reportStatus: { x: 255, y: 421 } },
        boundaries: { smeltr: { x: 30, y: 240, w: 300, h: 310 } },
        paths: ['M60 50H15V330H50', 'M300 180H345V490H310', 'M180 530V580'],
        controlPaths: ['M180 450V380'],
      },
    ],
  }),
  single: diagram({
    title: 'Standalone: leader and worker on one host',
    description:
      'An administrator connects to the leader. One Smeltr service runs the leader and one worker together, shown as joined cards. Sources send records through the worker to destinations.',
    caption:
      'One binary, one service: the leader and one worker run together. The administrator uses the leader; the worker reads from sources, filters records, and delivers them to destinations.',
    nodes: {
      administrator: {
        title: 'Administrator',
        detail: ['Edit · test · review'],
        role: 'administrator',
      },
      leader: { title: 'Leader', detail: ['UI + Git repository'], role: 'leader' },
      worker: {
        title: 'Worker',
        detail: ['Filter and deliver records'],
        role: 'worker',
        joined: true,
      },
      sources: { title: 'Sources', detail: ['Input records'], role: 'source' },
      destinations: { title: 'Destinations', detail: ['Kept records'], role: 'destination' },
    },
    boundaries: { oneService: 'One service' },
    layouts: [
      {
        width: 900,
        height: 380,
        nodes: {
          administrator: { x: 340, y: 10, w: 220 },
          leader: { x: 340, y: 160, w: 220 },
          worker: { x: 340, y: 240, w: 220 },
          sources: { x: 20, y: 240, w: 220 },
          destinations: { x: 660, y: 240, w: 220 },
        },
        boundaries: { oneService: { x: 310, y: 110, w: 280, h: 240 } },
        paths: ['M450 90V150', 'M240 280H330', 'M560 280H650'],
      },
      {
        width: 360,
        height: 630,
        nodes: {
          administrator: { x: 60, y: 10, w: 240 },
          sources: { x: 60, y: 140, w: 240 },
          leader: { x: 60, y: 300, w: 240 },
          worker: { x: 60, y: 380, w: 240 },
          destinations: { x: 60, y: 530, w: 240 },
        },
        boundaries: { oneService: { x: 30, y: 250, w: 300, h: 230 } },
        paths: ['M60 50H15V340H50', 'M300 180H345V420H310', 'M180 460V520'],
      },
    ],
  }),
  kubernetes: diagram({
    title: 'Smeltr on Kubernetes',
    description:
      'An administrator reaches the ingress over HTTPS. The ingress forwards through a private ClusterIP Service to one leader pod. Workers fetch main and report status through the ingress and connect directly to sources and destinations. The leader and each worker have persistent storage.',
    caption:
      'Dashed arrows show workers fetching main and reporting status through the ingress HTTPS address. The ingress forwards to the leader through a private ClusterIP Service. Dotted lines connect processes to persistent volumes: keep leader data and its secrets encryption key on separate volumes, and give each worker its own state volume. Back up the volumes. Sources and destinations may be inside or outside the cluster.',
    nodes: {
      administrator: { title: 'Administrator', detail: ['Browser · HTTPS'], role: 'administrator' },
      ingress: { title: 'Ingress', detail: ['HTTPS entry point'], role: 'ingress' },
      leader: { title: 'Leader', detail: ['One pod · private Service'], role: 'leader' },
      sources: { title: 'Sources', detail: ['Input records'], role: 'source' },
      workers: { title: 'Workers', detail: ['One process per pod'], role: 'worker' },
      destinations: { title: 'Destinations', detail: ['Kept records'], role: 'destination' },
      workerStorage: {
        title: 'Worker storage',
        detail: ['One volume per worker'],
        role: 'storage',
      },
      leaderStorage: {
        title: 'Leader storage',
        detail: ['Data + certificates', 'Separate secret-key volume'],
        role: 'storage',
      },
    },
    labels: { fetchMain: 'Fetch main', reportStatus: 'Report status', privateHttp: 'Private HTTP' },
    layouts: [
      {
        width: 900,
        height: 550,
        nodes: {
          administrator: { x: 20, y: 30, w: 220 },
          ingress: { x: 340, y: 30, w: 220 },
          leader: { x: 660, y: 30, w: 220 },
          sources: { x: 20, y: 310, w: 220 },
          workers: { x: 340, y: 310, w: 220 },
          destinations: { x: 660, y: 310, w: 220 },
          workerStorage: { x: 340, y: 440, w: 220 },
          leaderStorage: { x: 660, y: 160, w: 220 },
        },
        labels: {
          fetchMain: { x: 540, y: 210 },
          reportStatus: { x: 540, y: 231 },
          privateHttp: { x: 610, y: 95 },
        },
        paths: ['M240 70H330', 'M560 70H650', 'M240 350H330', 'M560 350H650'],
        controlPaths: ['M450 310V120'],
        storagePaths: ['M450 390V440', 'M770 110V160'],
      },
      {
        width: 360,
        height: 1050,
        nodes: {
          administrator: { x: 60, y: 10, w: 240 },
          ingress: { x: 60, y: 140, w: 240 },
          leader: { x: 60, y: 270, w: 240 },
          leaderStorage: { x: 60, y: 400, w: 240 },
          sources: { x: 60, y: 540, w: 240 },
          workers: { x: 60, y: 670, w: 240 },
          workerStorage: { x: 60, y: 800, w: 240 },
          destinations: { x: 60, y: 950, w: 240 },
        },
        labels: {
          fetchMain: { x: 290, y: 505 },
          reportStatus: { x: 290, y: 526 },
          privateHttp: { x: 235, y: 245 },
        },
        paths: ['M180 90V130', 'M180 220V260', 'M180 620V660', 'M60 710H15V990H50'],
        controlPaths: ['M300 710H345V180H310'],
        storagePaths: ['M180 350V400', 'M180 750V800'],
      },
    ],
  }),
  dataPath: diagram({
    title: 'From a source to its destinations, and back to the commit',
    description:
      "The reader polls a batch from the source and tracks its position. Transform routes the batch and runs its pipelines. Egress, one per destination, writes each destination's records and retries until they land. Each destination acknowledges its records, and once every destination the batch reached has acknowledged it, the reader commits the batch's position to the source.",
    caption:
      'A position is committed only after every destination the batch reached has acknowledged it, oldest batch first. After a restart or a retry a record can arrive twice, but it is not lost. Smeltr metrics and logs sources keep no position.',
    nodes: {
      source: { title: 'Source', detail: ['Kafka, metrics, logs'] },
      reader: { title: 'Reader', detail: ['Polls a batch,', 'tracks its position'] },
      transform: {
        title: 'Transform',
        detail: ['Routes, runs pipelines,', 'applies deliver rules'],
      },
      egress: {
        title: 'Egress',
        detail: ['One per destination,', 'retries until it lands'],
        stacked: true,
      },
      destination: { title: 'Destination', detail: ['Acknowledges', 'or refuses'], stacked: true },
      commit: { title: 'Commit', detail: ['Waits for every', "destination's ack"] },
    },
    labels: { acknowledged: 'acknowledged', commit: 'commit' },
    layouts: [
      {
        width: 900,
        height: 420,
        nodes: {
          source: { x: 20, y: 40, w: 180 },
          reader: { x: 250, y: 40, w: 180 },
          transform: { x: 480, y: 40, w: 190 },
          egress: { x: 720, y: 40, w: 160 },
          destination: { x: 720, y: 290, w: 160 },
          commit: { x: 250, y: 290, w: 180 },
        },
        labels: { acknowledged: { x: 580, y: 318 }, commit: { x: 160, y: 240 } },
        paths: ['M200 80H240', 'M430 80H470', 'M670 80H710', 'M800 120V264'],
        controlPaths: ['M720 330H440', 'M250 330H110V130'],
      },
      {
        width: 360,
        height: 600,
        nodes: {
          source: { x: 10, y: 20, w: 170 },
          reader: { x: 10, y: 140, w: 170 },
          transform: { x: 10, y: 260, w: 170 },
          egress: { x: 10, y: 380, w: 170 },
          destination: { x: 10, y: 500, w: 170 },
          commit: { x: 200, y: 260, w: 150 },
        },
        labels: { acknowledged: { x: 300, y: 445 }, commit: { x: 310, y: 160 } },
        paths: ['M95 100V130', 'M95 220V250', 'M95 340V362', 'M95 460V482'],
        controlPaths: ['M196 540H240V350', 'M275 260V60H190'],
      },
    ],
  }),
  remote: diagram({
    title: 'Follow a Git host',
    description:
      'A save in the Smeltr UI is a branch on the leader, which the leader pushes to the Git host. The host reviews and merges it to main. The leader fetches main, loads every worker group at it and deploys it only if all of them load. Workers fetch the deployed commit from the leader.',
    caption:
      'Branches go out to the host and main comes back. Workers only ever talk to the leader.',
    nodes: {
      gitHost: { title: 'Git host', detail: ['Review, merge to main'] },
      smeltrUi: { title: 'Smeltr UI', detail: ['Save to a branch'] },
      leader: { title: 'Leader', detail: ['Loads every group,', 'deploys if all load'] },
      workers: { title: 'Workers', detail: ['Fetch the deployed', 'commit'] },
    },
    labels: { pushBranches: 'push branches', fetchMain: 'fetch main' },
    layouts: [
      {
        width: 900,
        height: 290,
        nodes: {
          gitHost: { x: 300, y: 10, w: 200 },
          smeltrUi: { x: 10, y: 190, w: 170 },
          leader: { x: 300, y: 190, w: 200 },
          workers: { x: 620, y: 190, w: 260 },
        },
        labels: { pushBranches: { x: 285, y: 145 }, fetchMain: { x: 515, y: 145 } },
        paths: ['M360 190V100', 'M440 90V180', 'M180 230H290', 'M500 230H610'],
      },
      {
        width: 360,
        height: 390,
        nodes: {
          gitHost: { x: 80, y: 10, w: 200 },
          leader: { x: 80, y: 150, w: 200 },
          smeltrUi: { x: 10, y: 300, w: 160 },
          workers: { x: 190, y: 300, w: 160 },
        },
        labels: { pushBranches: { x: 75, y: 125 }, fetchMain: { x: 282, y: 125 } },
        paths: ['M140 150V100', 'M220 90V140', 'M90 300V240', 'M270 230V290'],
      },
    ],
  }),
  schemas: diagram({
    title: 'How schemas work',
    description:
      "A schema comes in from ECS, OCSF, a CSV or a JSON Schema. Smeltr checks a pipeline's records against it in the editor and in review. It can also be exported as an Elasticsearch mapping.",
    caption:
      "A schema comes in from a standard or a file. Smeltr checks a pipeline's records against it while you edit and in review. A mismatch is a finding, and never blocks a deploy. If you send to Elasticsearch, you can also export it as an index mapping.",
    nodes: {
      ecsOcsfCsv: { title: 'ECS · OCSF · CSV', detail: ['or a JSON Schema'] },
      schema: { title: 'Schema', detail: ['Fields · types · requirement'] },
      elasticsearch: { title: 'Elasticsearch', detail: ['Optional index mapping'] },
      pipelineEditor: { title: 'Pipeline editor', detail: ['Marks missing fields'] },
      review: { title: 'Review', detail: ['Lists findings'] },
    },
    labels: { read: 'Read', export: 'Export', checks: 'Checks' },
    layouts: [
      {
        width: 900,
        height: 300,
        nodes: {
          ecsOcsfCsv: { x: 20, y: 20, w: 220 },
          schema: { x: 340, y: 20, w: 220 },
          elasticsearch: { x: 660, y: 20, w: 220 },
          pipelineEditor: { x: 180, y: 200, w: 220 },
          review: { x: 500, y: 200, w: 220 },
        },
        labels: { read: { x: 285, y: 48 }, export: { x: 605, y: 48 }, checks: { x: 450, y: 140 } },
        paths: ['M240 60H330', 'M400 100V150H290V190', 'M500 100V150H610V190'],
        controlPaths: ['M560 60H650'],
      },
      {
        width: 360,
        height: 650,
        nodes: {
          ecsOcsfCsv: { x: 60, y: 10, w: 240 },
          schema: { x: 60, y: 150, w: 240 },
          pipelineEditor: { x: 60, y: 290, w: 240 },
          review: { x: 60, y: 410, w: 240 },
          elasticsearch: { x: 60, y: 560, w: 240 },
        },
        labels: {
          read: { x: 220, y: 118 },
          checks: { x: 222, y: 258 },
          export: { x: 160, y: 540 },
        },
        paths: ['M180 90V140', 'M180 230V280', 'M300 190H335V450H310'],
        controlPaths: ['M60 190H25V520H120V550'],
      },
    ],
  }),
} satisfies Record<string, Diagram>;
