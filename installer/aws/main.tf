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
resource "aws_vpc" "tightlock-vpc" {
  count = var.create_tightlock_network ? 1 : 0
  cidr_block = "10.0.0.0/16"  # Adjust CIDR block as needed
  tags = {
    Name = "tightlock-vpc"
  }
}

resource "aws_subnet" "tightlock-subnet" {
  count = var.create_tightlock_network ? 1 : 0
  vpc_id     = aws_vpc.tightlock-vpc[0].id
  cidr_block = "10.0.1.0/24"  # Adjust CIDR block as needed
  tags = {
    Name = "tightlock-subnet"
  }
}

resource "aws_key_pair" "dev_key" {
  key_name = "dev-key"
  public_key = "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQCrbbjF6stjIZBkRBaxo4F988Rz+pauvp+BysJqyZKyFCmNT4oF1k5NNtHgL4T3ynyVevggnH15jdctmef1wJHhMR9dsvqjTcPe+mpUfY/nFrRzx2L1YVkhzoyXEzgfUs1gx8ds+FNLYoAPVDbwuYwBe1v4YWahrxXPkWJ3FDgZ54wgVFiVeQ3A9IcWfp4WTM5EGhMsbpibPo1eBdwrA5I6jzRHLM8ry+g8ie3HeJCwzukjmJMjcdqUSkhZUrUKI610aUIRXqFHHPI3jG7CCFDUfyUHHCarwmGi7mNiibSb1nqE7FIMCRfCR3EaX7WO7WhCaln0tT15g7ULpGwv0p6KkLf1re/KDQRB0ELs77o51pYljQvQfqojN2xgWqNsXsiqrbBx59XGYkttKp1c8cuQ8bKgln9638ErYSJ2kziMFHtf9z/y5xj5JfV916cYWSbCnsK+ttvX/QlWwkCwZB4XegrdK8je7EgDBpofhRv0lRb5gT/WoEAT8a9HWS833bk= cloudshell-user@ip-10-134-90-139.us-east-2.compute.internal"
}

resource "aws_security_group" "tightlock-security-group" {
  name   = "tightlock-sg"
    # vpc_id = aws_vpc.tightlock-vpc[0].id

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
  # instance_type = "t3.medium"  # Map to equivalent AWS instance type
  instance_type = "t2.micro"  # Map to equivalent AWS instance type
  key_name = "ssh-test"
  user_data = templatefile("cloud-config.yaml", { API_KEY = "${var.api_key}"})
  vpc_security_group_ids = [aws_security_group.tightlock-security-group.id]
  # subnet_id              = aws_subnet.tightlock-subnet[0].id
  tags = {
    Name = format("tightlock-backend-%s", random_string.backend_name.result)
  }
}

resource aws_volume_attachment "tightlock-storage-att" {
  device_name = "/dev/sdt"
  volume_id = aws_ebs_volume.tightlock-storage.id
  instance_id = aws_instance.tightlock-backend.id
}

output "Test" {
  value = aws_instance.tightlock-backend
}

output "Connection_Code" {
  value = base64encode("{\"apiKey\": \"${var.api_key}\", \"address\": \"${aws_instance.tightlock-backend.public_ip}\"}")
}

output "Address" {
  value = aws_instance.tightlock-backend.public_ip
}
