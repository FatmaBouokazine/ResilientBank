#!/usr/bin/env bash
# Bootstraps ArgoCD into the current cluster.
#
# WHY UPSTREAM MANIFESTS ARE PINNED RATHER THAN HAND-WRITTEN:
# An earlier attempt hand-wrote a minimal controller Deployment. It applied cleanly and
# reported a failure only at runtime: the controller is fatal without the argocd-cm
# ConfigMap, and it cannot list applications.argoproj.io without the CRDs. A control plane
# that starts and looks plausible while being unable to sync anything is worse than one
# that visibly fails to install.
#
# ArgoCD has many interdependent required resources. Maintaining a hand-written subset is
# how clusters end up with a silently broken GitOps pipeline. This script therefore installs
# the upstream release manifest at a FIXED version, so what runs is reproducible, and the
# version lives in this repository where a pull request can change it.
set -euo pipefail

ARGOCD_VERSION="${ARGOCD_VERSION:-v2.13.0}"
NAMESPACE="${ARGOCD_NAMESPACE:-argocd}"
MANIFEST_URL="https://raw.githubusercontent.com/argoproj/argo-cd/${ARGOCD_VERSION}/manifests/install.yaml"

log() { echo "[argocd-bootstrap] $*"; }

# Verify kubectl points where we think it does. Installing a control plane into the wrong
# cluster is disruptive and slow to undo.
if ! kubectl cluster-info >/dev/null 2>&1; then
    log "ERROR: cannot reach a cluster. Start one with 'minikube start'."
    exit 1
fi
log "target cluster: $(kubectl config current-context 2>/dev/null || echo unknown)"

log "installing ArgoCD ${ARGOCD_VERSION} into namespace ${NAMESPACE}"
kubectl create namespace "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -

log "fetching upstream manifest"
# Pipe straight to kubectl: the manifest is version-pinned by URL, so re-running is
# idempotent and always installs exactly the pinned release.
curl -fsSL "${MANIFEST_URL}" | kubectl apply -n "${NAMESPACE}" -f -

log "waiting for the application controller to become available"
ready=0
for _ in $(seq 1 60); do
    if kubectl -n "${NAMESPACE}" wait --for=condition=Available \
        deployment/argocd-application-controller --timeout=10s >/dev/null 2>&1; then
        ready=1
        break
    fi
    sleep 5
done

if [ "$ready" != "1" ]; then
    log "ERROR: application controller did not become available in 300s"
    kubectl -n "${NAMESPACE}" get pods 2>&1 | sed 's/^/    /'
    kubectl -n "${NAMESPACE}" logs deploy/argocd-application-controller --tail=20 2>&1 | sed 's/^/    /'
    exit 1
fi

log "ArgoCD ${ARGOCD_VERSION} is running"
kubectl -n "${NAMESPACE}" get pods -o wide | sed 's/^/  /'