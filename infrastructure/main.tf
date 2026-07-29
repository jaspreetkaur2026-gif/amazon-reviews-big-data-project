terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }

  required_version = ">= 1.5"
}

provider "aws" {
  region = "us-east-1"
}

resource "aws_s3_bucket" "project_bucket" {
  bucket = "jaspreet-cs675-amazon-reviews-2026"

  tags = {
    Project = "CS675"
    Owner   = "Jaspreet Kaur"
  }
}

resource "aws_athena_workgroup" "project" {
  name = "cs675-workgroup"

  configuration {
    result_configuration {
      output_location = "s3://jaspreet-cs675-amazon-reviews-2026/athena-results/"
    }
  }
}