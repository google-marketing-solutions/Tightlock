# Provider block for AWS
terraform {
  required_providers {
    aws = {
      source  = "aws"
      version = "~> 4.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
  access_key = var.access_key
  secret_key = var.secret_key
}

# Equivalent for random_string
resource "random_string" "backend_name" {
  length  = 4
  special = false
  lower   = true
  upper   = false
}

# Network resources
# resource "aws_vpc" "tightlock-vpc" {
#   cidr_block = "10.0.0.0/16"  # Adjust CIDR block as needed
#   tags = {
#     Name = "tightlock-vpc"
#   }
# }

# resource "aws_subnet" "tightlock-subnet" {
#   vpc_id     = aws_vpc.tightlock-vpc.id
#   cidr_block = "10.0.0.0/16"  # Adjust CIDR block as needed
#   tags = {
#     Name = "tightlock-subnet"
#   }
# }

resource "aws_security_group" "tightlock-security-group" {
  name   = "tightlock-sg"
  # vpc_id = aws_vpc.tightlock-vpc.id

  ingress {
    protocol    = "tcp"
    from_port   = 80
    to_port     = 80
    cidr_blocks = ["0.0.0.0/0"]
    #cidr_blocks = ["35.199.32.68/32"]  # Allow connections from 1pd-scheduler.dev
  }


  ingress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress{
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# Storage resources
resource "aws_ebs_volume" "tightlock-storage" {
  availability_zone = var.aws_availability_zone
  size              = 20
  type              = "gp2"  # Change type if needed
  tags = {
    Name = format("tightlock-%s-storage", random_string.backend_name.result)
  }
}

# Instance resources
resource "aws_instance" "tightlock-backend" {
  ami           = "ami-0c2f3d2ee24929520"  # Replace with a suitable Amazon Linux 2 AMI
  availability_zone = var.aws_availability_zone
  associate_public_ip_address = true
  instance_type = "t2.large"  # Map to equivalent AWS instance type
  # instance_type = "t2.micro"  # Map to equivalent AWS instance type
  key_name = "tightlock"
  user_data = templatefile("cloud-config.yaml", { API_KEY = "${var.api_key}"})
  vpc_security_group_ids = [aws_security_group.tightlock-security-group.id]
  # subnet_id              = aws_subnet.tightlock-subnet.id
  tags = {
    Name = format("tightlock-backend-%s", random_string.backend_name.result)
  }
}

resource aws_volume_attachment "tightlock-storage-att" {
  device_name = "/dev/sdt"
  volume_id = aws_ebs_volume.tightlock-storage.id
  instance_id = aws_instance.tightlock-backend.id
  stop_instance_before_detaching = true
}

output "Connection_Code" {
  value = base64encode("{\"apiKey\": \"${var.api_key}\", \"address\": \"${aws_instance.tightlock-backend.public_ip}\"}")
}

output "Address" {
  value = aws_instance.tightlock-backend.public_ip
}
