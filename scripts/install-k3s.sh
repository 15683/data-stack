#!/usr/bin/env bash
# Run only on the target Ubuntu VM, not on an existing unrelated cluster.
set -euo pipefail
[[ $EUID -eq 0 ]] || { echo 'Run using sudo' >&2; exit 1; }
if [[ -e /etc/rancher/k3s/config.yaml || -e /usr/local/bin/k3s ]]; then
  echo 'Existing k3s installation/configuration detected; refusing to overwrite it.' >&2
  exit 1
fi
if [[ -n $(swapon --show=NAME --noheadings) ]]; then
  echo 'Disable swap and its boot configuration before installing k3s.' >&2
  exit 1
fi
K3S_VERSION=${K3S_VERSION:-v1.35.5+k3s1}
install -d -m 700 /etc/rancher/k3s
cat > /etc/rancher/k3s/config.yaml <<'YAML'
write-kubeconfig-mode: "0600"
secrets-encryption: true
disable:
  - traefik
  - servicelb
kubelet-arg:
  - "system-reserved=cpu=300m,memory=512Mi"
  - "kube-reserved=cpu=300m,memory=512Mi"
  - "eviction-hard=memory.available<350Mi,nodefs.available<10%,imagefs.available<10%"
  - "image-gc-high-threshold=70"
  - "image-gc-low-threshold=60"
  - "container-log-max-size=5Mi"
  - "container-log-max-files=2"
YAML
installer=$(mktemp)
trap 'rm -f "$installer"' EXIT
curl --fail --show-error --silent --location https://get.k3s.io -o "$installer"
INSTALL_K3S_VERSION="$K3S_VERSION" sh "$installer"
k3s kubectl wait --for=condition=Ready node --all --timeout=180s
echo 'Install Helm 3 on the VM, prepare kubeconfig and namespace, then follow README.'
