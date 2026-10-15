# Terraform base for the ResilientBank development cluster.
#
# SCOPE, AND WHY IT IS NARROWER THAN THE ROADMAP:
# ADR-free decision, recorded here deliberately. The roadmap's Phase 9 describes "Terraform
# cloud infrastructure", which for a real deployment means VPCs, node pools, and a managed
# Kubernetes control plane. None of that can be created or verified from this repository:
# it needs cloud credentials that must never live in git, and untested infrastructure code
# that provisions billable resources is worse than none.
#
# So this stack deliberately manages only what can be created AND verified locally against a
# real cluster (minikube): the namespaces the platform runs in, and the ArgoCD installation
# that deploys the application. Cloud landing-zone modules stay out until CI has a
# credentials mechanism and a way to test plans against a sandbox account.

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.30"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.13"
    }
  }

  # State is deliberately LOCAL, not a remote backend.
  #
  # A remote backend (S3, Terraform Cloud, the kubernetes backend) would make `terraform
  # init` fail on any machine without credentials for it, which means a fresh clone could
  # not even initialise. Local state keeps onboarding and CI working with no cloud account.
  # Moving state remotely is a later, deliberate change: once more than one person applies
  # this stack, local state will cause conflicting writes, and that is the moment to move.
}

provider "kubernetes" {
  # Resolved from the active kubeconfig context. Never hard-coded: the same Terraform must
  # work against minikube locally and a real cluster in CI.
  config_path    = var.kubeconfig_path
  config_context = var.kube_context
}

provider "helm" {
  kubernetes {
    config_path    = var.kubeconfig_path
    config_context = var.kube_context
  }
}

variable "kubeconfig_path" {
  description = "Path to the kubeconfig used to reach the target cluster."
  type        = string
  default     = null
}

variable "kube_context" {
  description = "kubeconfig context to operate against. Null uses the current context."
  type        = string
  default     = null
}

variable "app_namespace" {
  description = "Namespace the payment services are deployed into."
  type        = string
  default     = "resilientbank"
}

variable "argocd_namespace" {
  description = "Namespace ArgoCD itself runs in. Kept separate from the application."
  type        = string
  default     = "argocd"
}

variable "app_version" {
  description = "Image tag deployed for the payment services. Bumped by CI on release."
  type        = string
  default     = "latest"
}

# The platforms' own health gate. A resource is only marked healthy once the application
# reports ready, so a rollout that compiles but cannot serve traffic is treated as failed.
resource "kubernetes_namespace" "app" {
  metadata {
    name = var.app_namespace
    labels = {
      "app.kubernetes.io/part-of"    = "resilientbank"
      "resilientbank.io/managed-by"  = "terraform"
    }
  }
}

resource "kubernetes_namespace" "argocd" {
  metadata {
    name = var.argocd_namespace
    labels = {
      "app.kubernetes.io/part-of"   = "resilientbank"
      "resilientbank.io/managed-by" = "terraform"
    }
  }
}

# Opaque values only. No payment data, no real endpoints. The relay layer's correctness
# depends on this stack never pointing at production.
resource "kubernetes_config_map" "app" {
  metadata {
    name      = "resilientbank-config"
    namespace = kubernetes_namespace.app.metadata[0].name
  }

  data = {
    GRPC_ADDR    = ":50051"
    METRICS_ADDR = ":2112"

    # In-cluster addresses. The kafka listener is the broker's INTERNAL advertised name:
    # localhost inside a pod would point at that pod, not the broker.
    DATABASE_URL         = "postgres://resilient:resilient@postgres:5432/payments?sslmode=disable"
    REDIS_ADDR           = "redis:6379"
    KAFKA_BROKERS        = "kafka:29092"
    KAFKA_TOPIC_EVENTS   = "payments.events.v1"
    KAFKA_TOPIC_DLQ      = "payments.events.v1.dlq"
    KAFKA_ENABLED        = "true"
    OUTBOX_POLL_INTERVAL = "500ms"
    OUTBOX_BATCH_SIZE    = "100"
  }
}

output "app_namespace" {
  description = "Namespace the application is deployed into."
  value       = kubernetes_namespace.app.metadata[0].name
}

output "argocd_namespace" {
  description = "Namespace ArgoCD runs in."
  value       = kubernetes_namespace.argocd.metadata[0].name
}

output "argocd_service" {
  description = "Cluster-internal ArgoCD API endpoint, used by the Application CRDs."
  value       = "http://argocd-server.${var.argocd_namespace}.svc.cluster.local:80"
}

output "next_step" {
  description = "What must happen after this stack is applied."
  value       = "ArgoCD is installed but not yet managing ${var.app_namespace}. Apply the argocd/ Application manifests next, or let ArgoCD self-manage them."
}