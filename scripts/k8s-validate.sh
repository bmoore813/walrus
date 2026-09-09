#!/usr/bin/env bash
# k8s-validate.sh — manifest gate. Renders the kustomize base and schema-validates every
# resource with kubeconform, plus the standalone Secret template and explicit DuckLake migration Job.
# Same command CI runs,
# so "green locally" predicts "green in CI".
#
#   bash scripts/k8s-validate.sh
#
# Needs: kustomize (or `kubectl kustomize`) and kubeconform on PATH.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

BASE="deploy/k8s/base"
K8S_VERSION="${K8S_VERSION:-1.29.0}" # pin the API version so drift in the CI runner can't change results

# LoadRestrictionsNone: the slot-init ConfigMap is generated from ../../../migrations/source/*.sql
# (single source of truth), which lives outside the kustomize root.
render() {
  if command -v kustomize >/dev/null 2>&1; then
    kustomize build --load-restrictor LoadRestrictionsNone "$BASE"
  else
    kubectl kustomize --load-restrictor LoadRestrictionsNone "$BASE"
  fi
}

echo "== rendering $BASE =="
render >/tmp/walrus-k8s-rendered.yaml
echo "   $(grep -c '^kind:' /tmp/walrus-k8s-rendered.yaml) resources rendered"

echo "== kubeconform (strict, k8s $K8S_VERSION) on the rendered base =="
render | kubeconform -strict -summary -kubernetes-version "$K8S_VERSION" -

echo "== kubeconform on secrets.example.yaml (not part of the base) =="
kubeconform -strict -summary -kubernetes-version "$K8S_VERSION" "$BASE/secrets.example.yaml"

echo "== kubeconform on DuckLake catalog migration Job (explicit release step) =="
kubeconform -strict -summary -kubernetes-version "$K8S_VERSION" \
  deploy/k8s/ducklake-catalog-migrate-job.yaml

echo "k8s-validate: PASS"
