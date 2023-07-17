/*
 Copyright 2023 Google LLC

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

      https://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

resource "random_string" "backend_name" {
  length  = 4
  special = false
  lower   = true
  upper   = false
}

resource "google_project_service" "compute" {
  project            = var.project_id
  disable_on_destroy = false
  service            = "compute.googleapis.com"
}

resource "google_project_service" "cloudresourcemanager" {
  project            = var.project_id
  disable_on_destroy = false
  service            = "cloudresourcemanager.googleapis.com"
}

resource "google_project_service" "iam" {
  project            = var.project_id
  disable_on_destroy = false
  service            = "iam.googleapis.com"
}

data "google_compute_default_service_account" "default" {
  project = var.project_id
  depends_on = [
    google_project_service.cloudresourcemanager,
    google_project_service.compute,
    google_project_service.iam
  ]
}

resource "google_compute_network" "tightlock-network" {
  project = var.project_id
  name = format("tightlock-%s-network", random_string.backend_name.result)
}

resource "google_compute_firewall" "tightlock-firewall" {
  name = format("tightlock-%s-firewall", random_string.backend_name.result)
  network = google_compute_network.tightlock-network.name

  source_ranges = ["0.0.0.0/0"]

  allow {
    protocol = "tcp"
    ports    = ["80"]
  }

  source_tags = [format("tightlock-%s-tag", random_string.backend_name.result)]
}

resource "google_compute_disk" "tightlock-storage" {
  project = var.project_id
  name    = format("tightlock-%s-storage", random_string.backend_name.result)
  type    = "pd-ssd"
  zone    = var.compute_engine_zone
  size    = 50
  depends_on = [
    google_project_service.cloudresourcemanager,
    google_project_service.compute
  ]
}

resource "google_compute_address" "vm-static-ip" {
  name    = format("tightlock-%s-static-ip", random_string.backend_name.result)
  project = var.project_id
  region  = var.compute_address_region
  depends_on = [
    google_project_service.cloudresourcemanager,
    google_project_service.compute
  ]
}

resource "google_compute_instance" "tightlock-backend" {
  name                      = format("tightlock-backend-%s", random_string.backend_name.result)
  machine_type              = "e2-standard-4"
  zone                      = var.compute_engine_zone
  project                   = var.project_id
  tags                      = ["tightlock-tag"]
  allow_stopping_for_update = true
  deletion_protection       = false

  boot_disk {
    initialize_params {
      image = "cos-cloud/cos-105-17412-1-75"
    }
  }

  attached_disk {
    source      = google_compute_disk.tightlock-storage.self_link
    device_name = local.storage_device_name
  }

  network_interface {
    network = "default"
    access_config {
      nat_ip = google_compute_address.vm-static-ip.address
    }
  }

  metadata = {
    user-data = templatefile("cloud-config.yaml", { API_KEY = "${var.api_key}", STORAGE_DEVICE_NAME = "${local.storage_device_name}" })
  }

  service_account {
    email  = data.google_compute_default_service_account.default.email
    scopes = ["cloud-platform"]
  }

  depends_on = [
    google_compute_address.vm-static-ip,
    google_compute_disk.tightlock-storage
  ]
}

output "Compute_Engine_Instance" {
  value = google_compute_instance.tightlock-backend.name
}

output "Connection_Code" {
  value = base64encode("{\"apiKey\": \"${var.api_key}\", \"address\": \"${google_compute_address.vm-static-ip.address}\"}")
}

output "Address" {
  value = google_compute_address.vm-static-ip.address
}
