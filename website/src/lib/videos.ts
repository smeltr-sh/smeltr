/** Each video has one level. Learn lists videos easiest level first. */
export const levels = ['Beginner', 'Intermediate', 'Advanced'] as const;

/** Who a video is for. A security analyst uses the data downstream and looks at pipelines to see how a record
 * became what arrived; a data analyst edits pipelines; a data engineer connects sources and destinations; an
 * administrator runs smeltr. */
export const roles = [
  'Security analyst',
  'Data analyst',
  'Data engineer',
  'Administrator',
] as const;

export const topics = [
  'Access',
  'AI agent',
  'Demo',
  'Deploy',
  'Docker',
  'Exceptions',
  'Git',
  'HTTPS',
  'Install',
  'Kubernetes',
  'Lookups',
  'Parsing',
  'Pipelines',
  'Review',
  'Schemas',
  'Status',
  'Transform',
  'Workers',
] as const;

/** A training video. One with no `youtube` id is not out yet: its course lists it as coming soon. */
export type Video = {
  title: string;
  /** The video's page is /learn/{course}/{slug}. */
  slug: string;
  youtube?: string;
  minutes?: number;
  poster?: string;
  level: (typeof levels)[number];
  roles: (typeof roles)[number][];
  topics: (typeof topics)[number][];
};

/** Videos watched in order. The course's page is /learn/{slug}. */
export type Course = {
  title: string;
  slug: string;
  /** The demo has what the course shows, so the course links to it. */
  demo?: true;
  videos: Video[];
};

/** The video the home page shows. */
export const walkthrough: Video = {
  title: 'Fixing a feed in smeltr',
  slug: 'fixing-a-feed',
  level: 'Beginner',
  roles: ['Security analyst', 'Data analyst'],
  topics: ['Demo', 'Exceptions', 'Parsing', 'Pipelines'],
  youtube: 't_6-JqXfFtM',
  minutes: 6,
  poster: '/videos/walkthrough-poster.jpg',
};

const allCourses: Course[] = [
  {
    title: 'Demo walkthrough',
    slug: 'demo-walkthrough',
    demo: true,
    videos: [walkthrough],
  },
  {
    title: 'Getting started',
    slug: 'getting-started',
    videos: [
      {
        title: 'Install smeltr',
        slug: 'install',
        level: 'Beginner',
        roles: ['Administrator'],
        topics: ['Docker', 'Install'],
        youtube: '5h2pd3w98IA',
        minutes: 2,
        poster: '/videos/getting-started-1-poster.jpg',
      },
      {
        title: 'Enroll a worker',
        slug: 'enroll-a-worker',
        level: 'Beginner',
        roles: ['Administrator'],
        topics: ['Docker', 'Install', 'Workers'],
        youtube: 'rQ2lwZWkN4U',
        minutes: 2,
        poster: '/videos/getting-started-2-poster.jpg',
      },
      {
        title: 'Build your first pipeline',
        slug: 'first-pipeline',
        level: 'Beginner',
        roles: ['Data analyst', 'Data engineer'],
        topics: ['Pipelines'],
        youtube: 'bGxqdr5B06E',
        minutes: 4,
        poster: '/videos/getting-started-3-poster.jpg',
      },
      {
        title: 'Review and deploy a change',
        slug: 'review-and-deploy',
        level: 'Beginner',
        roles: ['Data analyst', 'Data engineer'],
        topics: ['Deploy', 'Review'],
        youtube: 'MAtSWX5Hznc',
        minutes: 2,
        poster: '/videos/getting-started-4-poster.jpg',
      },
    ],
  },
  {
    title: 'Data engineering with the smeltr agent',
    slug: 'agent',
    videos: [
      {
        title: 'Set Up Your AI Data Engineer in smeltr',
        slug: 'setup',
        level: 'Beginner',
        roles: ['Administrator'],
        topics: ['AI agent'],
        youtube: 'DiXMyUMlLWw',
        minutes: 3,
        poster: '/videos/agent-course-1-poster.jpg',
      },
      {
        title: 'AI Agent Builds a FortiGate to Elasticsearch Pipeline',
        slug: 'build-a-pipeline',
        level: 'Beginner',
        roles: ['Data analyst', 'Data engineer'],
        topics: ['AI agent', 'Parsing', 'Pipelines'],
        youtube: 'QqPgbVsE2I0',
        minutes: 3,
        poster: '/videos/agent-course-2-poster.jpg',
      },
      {
        title: 'Review and Deploy What Your AI Agent Built',
        slug: 'review-and-deploy',
        level: 'Beginner',
        roles: ['Data analyst', 'Data engineer'],
        topics: ['AI agent', 'Deploy', 'Review'],
        youtube: 'P8AqcVGqBxo',
        minutes: 5,
        poster: '/videos/agent-course-3-poster.jpg',
      },
      {
        title: 'Ask Your AI Agent About Your Fleet',
        slug: 'ask-about-your-fleet',
        level: 'Beginner',
        roles: ['Data engineer'],
        topics: ['AI agent', 'Status'],
        youtube: '48eIRNY_K50',
        minutes: 2,
        poster: '/videos/agent-course-4-poster.jpg',
      },
      {
        title: 'Have Your AI Agent Fix a Pipeline Problem',
        slug: 'fix-a-problem',
        level: 'Intermediate',
        roles: ['Data analyst', 'Data engineer'],
        topics: ['AI agent', 'Exceptions'],
        youtube: 'yXhaDPrdRc4',
        minutes: 3,
        poster: '/videos/agent-course-5-poster.jpg',
      },
    ],
  },
  {
    title: 'Trusted HTTPS',
    slug: 'trusted-https',
    videos: [
      {
        title: "Trust your leader's certificate",
        slug: 'trust-your-leaders-certificate',
        level: 'Beginner',
        roles: ['Administrator'],
        topics: ['HTTPS'],
        youtube: 'wX9O5T6-VNk',
        minutes: 2,
        poster: '/videos/trusted-https-1-poster.jpg',
      },
      {
        title: 'Order a certificate over ACME',
        slug: 'order-a-certificate-over-acme',
        level: 'Advanced',
        roles: ['Administrator'],
        topics: ['HTTPS'],
        youtube: 'J48N4nNLh0Y',
        minutes: 3,
        poster: '/videos/trusted-https-2-poster.jpg',
      },
      {
        title: 'Put a reverse proxy in front',
        slug: 'put-a-reverse-proxy-in-front',
        level: 'Advanced',
        roles: ['Administrator'],
        topics: ['HTTPS'],
        youtube: '6KF244WjVeU',
        minutes: 2,
        poster: '/videos/trusted-https-3-poster.jpg',
      },
    ],
  },
  {
    title: 'Shape your data',
    slug: 'shape-your-data',
    videos: [
      {
        title: 'Parse raw text',
        slug: 'parse-raw-text',
        level: 'Intermediate',
        roles: ['Security analyst', 'Data analyst'],
        topics: ['Parsing', 'Pipelines'],
        youtube: 'jFOLnXjTI00',
        minutes: 3,
        poster: '/videos/shape-your-data-1-poster.jpg',
      },
      {
        title: 'Transform with steps and code',
        slug: 'transform-with-steps-and-code',
        level: 'Intermediate',
        roles: ['Security analyst', 'Data analyst'],
        topics: ['Pipelines', 'Transform'],
        youtube: 'pMNWXEH30BA',
        minutes: 2,
        poster: '/videos/shape-your-data-2-poster.jpg',
      },
      {
        title: 'Enrich with lookups',
        slug: 'enrich-with-lookups',
        level: 'Intermediate',
        roles: ['Security analyst', 'Data analyst'],
        topics: ['Lookups', 'Pipelines'],
        youtube: '0XVgPvPXBV4',
        minutes: 3,
        poster: '/videos/shape-your-data-3-poster.jpg',
      },
      {
        title: 'Hold to a schema',
        slug: 'hold-to-a-schema',
        level: 'Advanced',
        roles: ['Security analyst', 'Data analyst'],
        topics: ['Pipelines', 'Schemas'],
        youtube: 'gRZAyKU5O1k',
        minutes: 3,
        poster: '/videos/shape-your-data-4-poster.jpg',
      },
    ],
  },
  {
    title: 'Work as a team',
    slug: 'work-as-a-team',
    videos: [
      {
        title: 'Give a teammate access',
        slug: 'give-a-teammate-access',
        level: 'Beginner',
        roles: ['Administrator'],
        topics: ['Access'],
        youtube: 'gwTY2LbT-pI',
        minutes: 2,
        poster: '/videos/work-as-a-team-1-poster.jpg',
      },
      {
        title: "Review a teammate's change",
        slug: 'review-a-teammates-change',
        level: 'Intermediate',
        roles: ['Data analyst'],
        topics: ['Access', 'Deploy', 'Review'],
        youtube: '5WFBNojgMaw',
        minutes: 4,
        poster: '/videos/work-as-a-team-2-poster.jpg',
      },
      {
        title: 'Follow a GitHub repository',
        slug: 'follow-a-github-repository',
        level: 'Advanced',
        roles: ['Administrator'],
        topics: ['Git'],
        youtube: 'h1WlIMw--cE',
        minutes: 3,
        poster: '/videos/work-as-a-team-3-poster.jpg',
      },
      {
        title: 'Approve from smeltr',
        slug: 'approve-from-smeltr',
        level: 'Advanced',
        roles: ['Administrator'],
        topics: ['Git', 'Review'],
        youtube: 'K5VT47ZBGNk',
        minutes: 2,
        poster: '/videos/work-as-a-team-4-poster.jpg',
      },
    ],
  },
  {
    title: 'Install on Kubernetes',
    slug: 'install-on-kubernetes',
    videos: [
      {
        title: 'Run the leader on Kubernetes',
        slug: 'run-the-leader-on-kubernetes',
        level: 'Advanced',
        roles: ['Administrator'],
        topics: ['HTTPS', 'Install', 'Kubernetes'],
        youtube: 'xqdWcGobXxw',
        minutes: 4,
        poster: '/videos/install-on-kubernetes-1-poster.jpg',
      },
      {
        title: 'Add workers that keep their identity',
        slug: 'add-workers-that-keep-their-identity',
        level: 'Advanced',
        roles: ['Administrator'],
        topics: ['Install', 'Kubernetes', 'Workers'],
        youtube: 'INKxB7IaPQo',
        minutes: 3,
        poster: '/videos/install-on-kubernetes-2-poster.jpg',
      },
      {
        title: 'Autoscale workers',
        slug: 'autoscale-workers',
        level: 'Advanced',
        roles: ['Administrator'],
        topics: ['Kubernetes', 'Workers'],
        youtube: '724W0fyyIdI',
        minutes: 3,
        poster: '/videos/install-on-kubernetes-3-poster.jpg',
      },
    ],
  },
  {
    title: 'Handle exceptions',
    slug: 'handle-exceptions',
    videos: [
      {
        title: 'Where bad records go',
        slug: 'where-bad-records-go',
        level: 'Intermediate',
        roles: ['Security analyst', 'Data analyst'],
        topics: ['Exceptions', 'Parsing'],
        youtube: 'UsoSUz6zyXE',
        minutes: 2,
        poster: '/videos/handle-exceptions-1-poster.jpg',
      },
      {
        title: 'When a source blocks',
        slug: 'when-a-source-blocks',
        level: 'Intermediate',
        roles: ['Data analyst', 'Data engineer', 'Administrator'],
        topics: ['Exceptions', 'Status'],
        youtube: '-EQPcOsrfDg',
        minutes: 3,
        poster: '/videos/handle-exceptions-2-poster.jpg',
      },
      {
        title: 'Route exceptions to a topic',
        slug: 'route-exceptions-to-a-topic',
        level: 'Intermediate',
        roles: ['Data analyst', 'Data engineer'],
        topics: ['Deploy', 'Exceptions'],
        youtube: 'eKbDQS7Wvrw',
        minutes: 2,
        poster: '/videos/handle-exceptions-3-poster.jpg',
      },
    ],
  },
];

export const released = (course: Course) => course.videos.filter((v) => v.youtube);

/** The courses the site shows: those with a video out. */
export const courses = allCourses.filter((c) => released(c).length);

/** How many videos the course has and how long the released ones run, as its cards say it. */
export function courseLength(course: Course): string {
  const out = released(course);
  const soon = course.videos.length - out.length;
  const minutes = out.reduce((sum, v) => sum + (v.minutes ?? 0), 0);
  const videos = out.length === 1 ? '1 video' : `${out.length} videos`;
  return `${videos}${soon ? `, ${soon} coming soon` : ''} · ${minutes} min`;
}
