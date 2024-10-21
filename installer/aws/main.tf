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

resource "random_string" "backend_name" {
  length  = 4
  special = false
  lower   = true
  upper   = false
}

resource "aws_iam_role" "tightlock_role" {
  name = "tightlock_role"

  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "ec2.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF
}

resource "aws_iam_role_policy_attachment" "tightlock_attachment" {
  role       = aws_iam_role.tightlock_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
}

resource "aws_iam_instance_profile" "tightlock_profile" {
  name = "tightlock_profile"
  role = aws_iam_role.tightlock_role.name
}

resource "aws_security_group" "tightlock-security-group" {
  name   = "tightlock-sg"

  ingress {
    protocol    = "tcp"
    from_port   = 0
    to_port     = 0
    cidr_blocks = ["0.0.0.0/0"]  # Allow connections from 1pd-scheduler.dev
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
  size              = 50
  type              = "gp2"  # Change type if needed
  tags = {
    Name = format("tightlock-%s-storage", random_string.backend_name.result)
  }
}

# Instance resources
resource "aws_instance" "tightlock-backend" {
  ami           = "ami-0c2f3d2ee24929520"  
  availability_zone = var.aws_availability_zone
  associate_public_ip_address = true
  instance_type = "t2.large"  
  key_name = "tightlock"
  user_data = templatefile("cloud-config.yaml", { API_KEY = "${var.api_key}", USAGE_COLLECTION = "${var.allow_usage_data_collection}" })
  vpc_security_group_ids = [aws_security_group.tightlock-security-group.id]
  iam_instance_profile = aws_iam_instance_profile.tightlock_profile.name
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
