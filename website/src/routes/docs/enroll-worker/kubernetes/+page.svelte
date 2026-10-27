<script lang="ts">
  import { image } from '#lib/release.ts';
  import GuideLayout from '#lib/components/GuideLayout.svelte';

  const sections = [
    { id: 'worker', title: 'Deploy a worker' },
    { id: 'autoscale', title: 'Autoscale workers' },
    { id: 'receiver', title: 'Expose a receiver' },
  ];
</script>

<GuideLayout
  title="Enroll workers on Kubernetes"
  description={`Deploy Smeltr workers on Kubernetes, persistent or autoscaled, from the ${image} image.`}
  {sections}
>
  <h1>Enroll workers on Kubernetes</h1>
  <p>
    Workers run the leader's image, named by the same digest, and connect to a leader deployed as in
    <a href="/docs/install-leader/kubernetes">Run the leader on Kubernetes</a>. Workers run either
    as a StatefulSet, each with its own persistent identity, or as a Deployment that an autoscaler
    grows and shrinks, each pod enrolling itself when it starts.
  </p>

  <h2 id="worker">Deploy a worker</h2>
  <p>
    First create and deploy the worker group in Smeltr, then mint a permanent enrollment code that
    brings in enough workers for the pods you are starting. Save this as <code
      >smeltr-worker.yaml</code
    >
    and replace the image digest, leader URL and enrollment code:
  </p>
  <pre><code
      >apiVersion: v1
kind: ConfigMap
metadata:
  name: smeltr-worker-default
data:
  smeltr.toml: |
    [worker]
    enabled = true
    group = "default"

    [worker.leader]
    url = "https://smeltr.example.com"

    [worker.storage]
    state_dir = "/var/lib/smeltr-worker"

    [worker.runtime]
    thread_limit = 2
---
apiVersion: v1
kind: Secret
metadata:
  name: smeltr-worker-default-enrollment-code
type: Opaque
stringData:
  enrollment-code: PASTE_THE_COMPLETE_ENROLLMENT_CODE_HERE
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: smeltr-worker-default
spec:
  serviceName: smeltr-worker-default
  replicas: 1
  selector:
    matchLabels:
      app: smeltr-worker-default
  template:
    metadata:
      labels:
        app: smeltr-worker-default
    spec:
      terminationGracePeriodSeconds: 430
      securityContext:
        runAsNonRoot: true
        runAsUser: 1000
        fsGroup: 1000
      containers:
        - name: smeltr
          image: {image}@sha256:REPLACE_WITH_DIGEST
          imagePullPolicy: IfNotPresent
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            capabilities:
              drop: [ALL]
            seccompProfile:
              type: RuntimeDefault
          env:
            - name: SMELTR_CONFIG_FILE
              value: /config/smeltr.toml
            - name: SMELTR_WORKER_ENROLLMENT_CODE
              valueFrom:
                secretKeyRef:
                  name: smeltr-worker-default-enrollment-code
                  key: enrollment-code
          resources:
            requests:
              cpu: "2"
              memory: 2Gi
            limits:
              memory: 2Gi
          volumeMounts:
            - name: config
              mountPath: /config
              readOnly: true
            - name: worker-state
              mountPath: /var/lib/smeltr-worker
      volumes:
        - name: config
          configMap:
            name: smeltr-worker-default
  volumeClaimTemplates:
    - metadata:
        name: worker-state
      spec:
        accessModes: ["ReadWriteOnce"]
        resources:
          requests:
            storage: 2Gi</code
    ></pre>
  <p>
    The StatefulSet gives each numbered worker pod its own persistent volume. Never share or clone
    those claims: each contains one worker identity and credential. Workers require outbound access
    to the public leader URL and to their configured sources and destinations. They require no
    Service or inbound port unless their group runs a
    <a href="/docs/sources#push-all-workers">receiver</a>; see
    <a href="#receiver">Expose a receiver</a>.
  </p>
  <p>
    <code>thread_limit</code> matches the CPU request, since a pod with no CPU limit otherwise runs
    a transform thread for every core on its node. Size the memory limit for what the group runs:
    see <a href="/docs/enroll-worker/sizing">Size a worker</a>.
  </p>
  <p>
    <code>terminationGracePeriodSeconds</code> gives a stopping worker time to stop cleanly. It gives
    its pipeline up to 90 seconds to drain before killing it, then tells the leader it is stopping, then
    exits. Most stops take seconds, and the pod ends when the worker exits. Every step is bounded. The
    slowest stop takes about seven minutes: a pipeline the worker must kill, a stop that lands mid-deploy,
    and a leader slow to answer. 430 seconds covers the slowest stop. Status shows a worker that stops
    this way as Stopped, and it raises no warning. A shorter grace period can end a worker mid-drain:
    what it read since its last commit is read again by whichever worker next owns it, and the leader
    reports the worker silent.
  </p>
  <p>
    After the worker appears Online, remove the <code>SMELTR_WORKER_ENROLLMENT_CODE</code> environment
    entry from the StatefulSet, apply the change, and confirm the restarted pod reconnects using its persisted
    credential. Then delete the enrollment code Secret. The worker needs it only to enroll.
  </p>

  <h2 id="autoscale">Autoscale workers</h2>
  <p>
    A group that grows and shrinks with load runs as a Deployment. Every pod enrolls itself with one
    ephemeral enrollment code, and the leader forgets each worker once it is gone. Create and deploy
    the group, then in Admin → Workers → Enrollment codes choose <strong
      >+ Add enrollment code</strong
    >, set
    <em>Workers it brings in</em> to Ephemeral and select <strong>Mint</strong>. An ephemeral
    enrollment code has no expiry and no limit on the workers it brings in: it works until you
    withdraw it. Save this as <code>smeltr-worker-ingest.yaml</code> and replace the image digest, leader
    URL and enrollment code:
  </p>
  <pre><code
      >apiVersion: v1
kind: ConfigMap
metadata:
  name: smeltr-worker-ingest
data:
  smeltr.toml: |
    [worker]
    enabled = true
    group = "ingest"

    [worker.leader]
    url = "https://smeltr.example.com"

    [worker.storage]
    state_dir = "/var/lib/smeltr-worker"

    [worker.runtime]
    thread_limit = 2
---
apiVersion: v1
kind: Secret
metadata:
  name: smeltr-worker-ingest-enrollment-code
type: Opaque
stringData:
  enrollment-code: PASTE_THE_COMPLETE_EPHEMERAL_ENROLLMENT_CODE_HERE
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: smeltr-worker-ingest
spec:
  replicas: 2
  selector:
    matchLabels:
      app: smeltr-worker-ingest
  template:
    metadata:
      labels:
        app: smeltr-worker-ingest
    spec:
      terminationGracePeriodSeconds: 430
      securityContext:
        runAsNonRoot: true
        runAsUser: 1000
        fsGroup: 1000
      containers:
        - name: smeltr
          image: {image}@sha256:REPLACE_WITH_DIGEST
          imagePullPolicy: IfNotPresent
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            capabilities:
              drop: [ALL]
            seccompProfile:
              type: RuntimeDefault
          env:
            - name: SMELTR_CONFIG_FILE
              value: /config/smeltr.toml
            - name: SMELTR_WORKER_ENROLLMENT_CODE
              valueFrom:
                secretKeyRef:
                  name: smeltr-worker-ingest-enrollment-code
                  key: enrollment-code
          resources:
            requests:
              cpu: "2"
              memory: 2Gi
            limits:
              memory: 2Gi
          volumeMounts:
            - name: config
              mountPath: /config
              readOnly: true
            - name: worker-state
              mountPath: /var/lib/smeltr-worker
      volumes:
        - name: config
          configMap:
            name: smeltr-worker-ingest
        - name: worker-state
          emptyDir: &#123;&#125;
---
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: smeltr-worker-ingest
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: smeltr-worker-ingest
  minReplicas: 2
  maxReplicas: 12
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: 70</code
    ></pre>
  <p>
    Keep the enrollment code Secret and its environment entry: every new pod enrolls with it. The
    state directory is an <code>emptyDir</code>, so a worker's identity lives and dies with its pod.
    A container that restarts in its pod keeps its identity; a pod that is replaced enrolls as a new
    worker, and clones the leader's configuration repository before it runs anything.
  </p>
  <p>
    A worker that stops cleanly tells the leader, which forgets it at once. The leader forgets a
    worker that is killed without stopping cleanly once it has been silent for the time set under
    Admin → Workers → Ephemeral workers, ten minutes by default. A worker the leader has forgotten
    that comes back, such as one cut off from the leader for longer than that, enrolls again with
    the enrollment code as a new worker and restarts as it.
  </p>
  <ul>
    <li>
      Keep <code>maxReplicas</code> at or below the partition count of the Kafka topics the group reads.
      A worker beyond that is assigned no partitions and sits idle.
    </li>
    <li>
      Do not autoscale a group whose Kafka source reads a selected range, one with a start or stop
      position. A range read shares no work: every pod reads the whole range.
    </li>
    <li>Set a CPU request for the HorizontalPodAutoscaler to scale on.</li>
    <li>
      The enrollment code enrolls a worker, and an enrolled worker can receive every credential the
      leader stores. Restrict who can read the Secret. If it is exposed, withdraw it in the
      Enrollment codes table and mint another: running workers keep their credentials, and new pods
      need the new one.
    </li>
    <li>
      Rotating the leader's certificate authority withdraws the enrollment code at cutover, and you
      cannot mint one while the rotation is open. Mint a new ephemeral enrollment code after cutover
      and update the Secret; until then new pods cannot enroll.
    </li>
  </ul>

  <h2 id="receiver">Expose a receiver</h2>
  <p>
    A receiver is a source that senders connect to. Every worker of its group listens on the
    receiver's <code>port</code>, so each worker pod needs that port open, and senders need one
    address that reaches all of them. Add the port to the worker container, beside its
    <code>env</code>. This example uses port 8088:
  </p>
  <pre><code>          ports:
            - name: receiver
              containerPort: 8088
          readinessProbe:
            httpGet:
              path: /health
              port: receiver
            periodSeconds: 5</code
    ></pre>
  <p>Then add a Service that selects the worker pods:</p>
  <pre><code
      >apiVersion: v1
kind: Service
metadata:
  name: smeltr-worker-default-receiver
spec:
  type: LoadBalancer
  selector:
    app: smeltr-worker-default
  ports:
    - name: receiver
      port: 8088
      targetPort: receiver</code
    ></pre>
  <p>
    Pick a
    <a
      href="https://kubernetes.io/docs/concepts/services-networking/service/#publishing-services-service-types"
      >Service type</a
    > for your balancer. For an autoscaled group, select its Deployment's pods.
  </p>
  <p>
    <code>GET /health</code> on the receiver port answers 200 while the receiver takes requests and 503
    while it is full, blocked or stopping, so the readiness probe takes a busy worker out of the Service
    until it can take requests again. The probe is optional.
  </p>
  <ul>
    <li>
      A receiver that names a certificate speaks TLS, so its probe adds <code>scheme: HTTPS</code>.
    </li>
    <li>
      With authorities, every connection to the receiver must present a client certificate, <code
        >/health</code
      >
      included. An <code>httpGet</code> probe has none, so use a
      <code>tcpSocket</code> probe on the receiver port there. It confirms only that the worker listens.
    </li>
    <li>
      The worker runs as a non-root user without capabilities and cannot listen on a port below
      1024. For syslog's 514, set the receiver's <code>port</code> to a high one, such as 5514, and
      give the Service <code>port: 514</code> with <code>targetPort: receiver</code>.
    </li>
    <li>
      A syslog receiver over UDP needs <code>protocol: UDP</code> on its container port and on the
      Service's, and has no health path, so it takes no probe. Its <code>allowFrom</code> sees the
      address a datagram arrives from, so the Service keeps the sender's own:
      <code>externalTrafficPolicy: Local</code> on a LoadBalancer or NodePort Service.
    </li>
  </ul>
</GuideLayout>
