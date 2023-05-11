resource "google_compute_address" "vm-static-ip" {
  name    = "vm-static-ip"
  project = "tightlock-dev-gke"
  region  = "us-central1"
}

resource "google_compute_instance" "tightlock-backend" {
  name         = "tightlock-backend"
  machine_type = "e2-standard-4"
  zone         = "us-central1-a"
  project = "tightlock-dev-gke"

 boot_disk {
    initialize_params {
      image = "cos-cloud/cos-105-17412-1-75"
    }
  }

 network_interface {
   network = "default"
   access_config {
     nat_ip = "${google_compute_address.vm-static-ip.address}"
   }
 }

 metadata = {
    user-data = file("cloud-config.yaml")
  }
}

