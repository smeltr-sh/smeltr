/** A topic on the Learn page. */
export type Topic = 'start' | 'build' | 'operate' | 'agent';

export const topicNames: Record<Topic, string> = {
  start: 'Get started',
  build: 'Build & test',
  operate: 'Deploy & operate',
  agent: 'Agent course',
};

/** A training video. One with no `youtube` id is planned: its card says so and links the guide that covers it today, if one does. */
export type Video = {
  topic: Topic;
  title: string;
  /** The video's page is /learn/{slug}. */
  slug: string;
  /** Linked from the card while the video is planned. */
  guide?: string;
  youtube?: string;
  minutes?: number;
  poster?: string;
};

/** The video the home page shows. */
export const walkthrough: Video = {
  topic: 'build',
  title: 'Fixing a feed in smeltr',
  slug: 'fixing-a-feed',
  guide: '/docs/first-pipeline',
  youtube: 't_6-JqXfFtM',
  minutes: 6,
  poster: '/videos/walkthrough-poster.jpg',
};

export const videos: Video[] = [
  { topic: 'start', title: 'Install smeltr', slug: 'install', guide: '/docs/getting-started' },
  walkthrough,
  {
    topic: 'operate',
    title: 'Review and deploy a change',
    slug: 'review-and-deploy',
    guide: '/docs/review',
  },
  {
    topic: 'agent',
    title: 'Set Up Your AI Data Engineer in smeltr',
    slug: 'agent-setup',
  },
  {
    topic: 'agent',
    title: 'AI Agent Builds a FortiGate to Elasticsearch Pipeline',
    slug: 'agent-builds-a-pipeline',
    youtube: 'QqPgbVsE2I0',
    minutes: 3,
    poster: '/videos/agent-course-2-poster.jpg',
  },
  {
    topic: 'agent',
    title: 'Review and Deploy What Your AI Agent Built',
    slug: 'agent-review-and-deploy',
    youtube: 'P8AqcVGqBxo',
    minutes: 5,
    poster: '/videos/agent-course-3-poster.jpg',
  },
];
