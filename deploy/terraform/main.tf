# Fusee on Kubernetes with Terraform: installs the Helm chart in
# deploy/helm/fusee. One member, or a replicated group of three.
#
#   terraform init
#   terraform apply -var members=3
#
# The token is generated here and kept in the Kubernetes Secret the chart
# reads; workers and clients get it from the output (sensitive).

terraform {
  required_version = ">= 1.5"
  required_providers {
    helm   = { source = "hashicorp/helm", version = "~> 2.13" }
    random = { source = "hashicorp/random", version = "~> 3.6" }
  }
}

variable "kubeconfig" {
  description = "Path to the kubeconfig of the target cluster."
  type        = string
  default     = "~/.kube/config"
}

variable "namespace" {
  type    = string
  default = "fusee"
}

variable "members" {
  description = "1 (one server) or an odd number (a replicated group; a majority must be up)."
  type        = number
  default     = 1
  validation {
    condition     = var.members == 1 || (var.members >= 3 && var.members % 2 == 1)
    error_message = "members is 1, or an odd number of at least 3."
  }
}

variable "storage_class" {
  description = "A storage class whose volumes honour flushes (local NVMe or a block volume)."
  type        = string
  default     = ""
}

variable "storage_size" {
  type    = string
  default = "20Gi"
}

provider "helm" {
  kubernetes {
    config_path = pathexpand(var.kubeconfig)
  }
}

resource "random_password" "token" {
  length  = 48
  special = false
}

resource "helm_release" "fusee" {
  name             = "fusee"
  namespace        = var.namespace
  create_namespace = true
  chart            = "${path.module}/../helm/fusee"

  set {
    name  = "members"
    value = var.members
  }
  set {
    name  = "storage.size"
    value = var.storage_size
  }
  set {
    name  = "storage.className"
    value = var.storage_class
  }
  set_sensitive {
    name  = "token"
    value = random_password.token.result
  }
}

output "address" {
  description = "Workers and clients connect here (inside the cluster)."
  value       = "fusee-fusee.${var.namespace}.svc:7600"
}

output "http" {
  description = "The HTTP API, UI and /metrics (inside the cluster; Bearer token)."
  value       = "http://fusee-fusee.${var.namespace}.svc:7601"
}

output "token" {
  value     = random_password.token.result
  sensitive = true
}
