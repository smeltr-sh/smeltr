<script lang="ts">
  import { image } from '#lib/release.ts';
  import KubernetesTopology from '#lib/components/KubernetesTopology.svelte';
  import GuideLayout from '#lib/components/GuideLayout.svelte';

  const sections = [
    { id: 'requirements', title: 'Requirements and image' },
    { id: 'leader', title: 'Deploy the leader' },
    { id: 'ingress', title: 'Expose the leader' },
  ];
</script>

<GuideLayout
  title="Run the leader on Kubernetes"
  description={`Deploy a persistent Smeltr leader on Kubernetes from the ${image} image.`}
  {sections}
>
  <h1>Run the leader on Kubernetes</h1>
  <p>
    This deployment runs the leader as one persistent replica. Smeltr does not provide a
    multi-replica or active/standby leader in this release. To run workers in the cluster, see
    <a href="/docs/enroll-worker/kubernetes">Enroll workers on Kubernetes</a>; to upgrade, see
    <a href="/docs/upgrade#kubernetes">Upgrade Smeltr</a>.
  </p>

  <KubernetesTopology />

  <h2 id="requirements">Requirements and image</h2>
  <p>Before deploying, provide:</p>
  <ul>
    <li>
      A Kubernetes cluster with a default or explicitly selected StorageClass that supports
      ReadWriteOnce persistent volumes.
    </li>
    <li>An ingress controller, DNS name and a TLS certificate that browsers and workers trust.</li>
    <li>
      Nodes that can pull from <code>ghcr.io</code>, or a registry your organization mirrors the
      image into.
    </li>
  </ul>
  <p>
    Pull the release's image, <a href="/docs/install-leader/container#pull">verify its signature</a
    >, and name it by that digest in the manifests:
    <a href="/docs/install-leader/container#latest">a tag can move to a newer image</a>, a digest
    cannot. Keep the digest and the release notes with the deployment. If you mirror the image,
    replace the whole image reference, registry and digest, in every manifest, the workers'
    included.
  </p>

  <h2 id="leader">Deploy the leader</h2>
  <p>
    Save the following as <code>smeltr-leader.yaml</code>. Replace the public hostname, image digest
    and storage requests for your environment. TLS terminates at the ingress, so Smeltr serves HTTP
    only inside the cluster.
  </p>
  <p>
    The port default is 443, which is privileged, and the pod runs as user 1000 -- so
    <code>leader.http.port</code> names an unprivileged port instead. Set it in the file rather than
    the environment: the file only backfills variables that are unset, so an environment variable
    wins over it. <code>fsGroup</code> matches the image's group, without which the process cannot write
    a freshly provisioned volume.
  </p>
  <p>
    The container's own <code>securityContext</code> meets the Kubernetes "restricted" Pod Security Standard:
    no privilege escalation, no capabilities, the runtime's default seccomp profile, and a read-only root
    filesystem. Smeltr writes only to its volumes, so nothing else needs to be writable.
  </p>
  <p>
    The leader makes its data directory and the sealing key's directory private to itself, and only
    a directory's owner can do that. A volume's root belongs to root whatever <code>fsGroup</code>
    says, so <code>data_dir</code> and <code>key</code> name directories inside their volumes, which the
    leader creates on first start.
  </p>
  <pre><code
      >apiVersion: v1
kind: ConfigMap
metadata:
  name: smeltr-leader
data:
  smeltr.toml: |
    [leader]
    enabled = true

    [leader.http]
    bind = "0.0.0.0"
    port = 4000
    public_origin = "https://smeltr.example.com"

    [leader.tls]
    enabled = false

    [leader.storage]
    data_dir = "/var/lib/smeltr/data"

    [leader.secrets]
    key = "/var/lib/smeltr-secrets/key/sealing.key"
---
apiVersion: v1
kind: Service
metadata:
  name: smeltr-leader
spec:
  type: ClusterIP
  selector:
    app: smeltr-leader
  ports:
    - name: http
      port: 4000
      targetPort: http
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: smeltr-leader
spec:
  serviceName: smeltr-leader
  replicas: 1
  selector:
    matchLabels:
      app: smeltr-leader
  template:
    metadata:
      labels:
        app: smeltr-leader
    spec:
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
          ports:
            - name: http
              containerPort: 4000
          volumeMounts:
            - name: config
              mountPath: /config
              readOnly: true
            - name: leader-data
              mountPath: /var/lib/smeltr
            - name: leader-secrets
              mountPath: /var/lib/smeltr-secrets
      volumes:
        - name: config
          configMap:
            name: smeltr-leader
  volumeClaimTemplates:
    - metadata:
        name: leader-data
      spec:
        accessModes: ["ReadWriteOnce"]
        resources:
          requests:
            storage: 10Gi
    - metadata:
        name: leader-secrets
      spec:
        accessModes: ["ReadWriteOnce"]
        resources:
          requests:
            storage: 1Gi</code
    ></pre>
  <p>
    Apply it in the intended namespace, wait for <code>smeltr-leader-0</code>, and inspect its
    console for startup errors:
  </p>
  <pre><code
      >kubectl apply -f smeltr-leader.yaml
kubectl rollout status statefulset/smeltr-leader
kubectl logs smeltr-leader-0</code
    ></pre>
  <p>
    A new leader prints the initial administrator username and one-time password to stderr. Treat
    the pod log as sensitive until you change that password. The data and sealing-key claims are
    deliberately separate; back them up as one matched recovery set.
  </p>

  <h2 id="ingress">Expose the leader</h2>
  <p>
    Create an Ingress using the same hostname as <code>leader.http.public_origin</code>. Controller
    annotations and certificate provisioning are cluster-specific:
  </p>
  <pre><code
      >apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: smeltr
spec:
  tls:
    - hosts:
        - smeltr.example.com
      secretName: smeltr-ingress-tls
  rules:
    - host: smeltr.example.com
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service:
                name: smeltr-leader
                port:
                  name: http</code
    ></pre>
  <p>
    Keep the Service as ClusterIP; only the ingress should be externally reachable. Configure the
    ingress to preserve the browser <code>Origin</code> header. Open the configured public origin URL,
    change the initial password, and create named administrator accounts. Because TLS terminates before
    Smeltr, workers use platform trust for the ingress certificate.
  </p>
</GuideLayout>
