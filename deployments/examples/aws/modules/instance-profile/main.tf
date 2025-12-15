# This module builds IAM related resources that are shared among resources:
#  - A default instance profile for the EC2 instances that run Springtail services
#  - An IAM role for Lambda functions that need to access VPC resources
locals {
  dummy_account_id = "000000000000"
}
data "aws_caller_identity" "current" {}

# -- Begin: EC2 Instance IAM Role and Profile
resource "aws_iam_role" "services_instance_role" {
  name = "${var.resource_name_prefix}-services-instance-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}
resource "aws_iam_policy" "basic_instance_policy" {
  name        = "${var.resource_name_prefix}-services-policy"
  description = "Policy to allow S3 and ECR access from container instances"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "sns:Subscribe",
          "sns:ConfirmSubscription",
          "sns:Unsubscribe",
        ],
        Resource = var.sns_topic_subscribe_arns == null ? ["*"] : var.sns_topic_subscribe_arns,
        Condition = {
          StringEquals = {
            "aws:PrincipalAccount" = (var.sns_topic_subscribe_arns == null ? local.dummy_account_id :
            data.aws_caller_identity.current.account_id)
          }
        }
      },

      {
        Effect = "Allow",
        Action = [
          "sns:Publish",
        ],
        Resource = var.sns_topic_publish_arns == null ? ["*"] : var.sns_topic_publish_arns,
        Condition = {
          StringEquals = {
            "aws:PrincipalAccount" = (var.sns_topic_publish_arns == null ? local.dummy_account_id :
            data.aws_caller_identity.current.account_id)
          }
        }
      },
      {
        Effect = "Allow"
        Action = [
          "ssmmessages:CreateControlChannel",
          "ssmmessages:CreateDataChannel",
          "ssmmessages:OpenControlChannel",
          "ssmmessages:OpenDataChannel",
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:DescribeLogStreams",
          "logs:DescribeLogGroups",
          "cloudwatch:PutMetricData",
          "cloudwatch:GetMetricData",
          "ec2:DescribeTags",
          "ec2:DescribeVolumes",
          "aps:QueryMetrics",
          "aps:RemoteWrite",
          "aps:ListWorkspaces",
          "aps:DescribeWorkspace",
        ]
        Resource = [
          "*"
        ]
      },
      {
        Effect = "Allow",
        Action = [
          "secretsmanager:GetSecretValue",
        ],
        Resource = [
          "*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "elasticloadbalancing:RegisterTargets",
          "elasticloadbalancing:DeregisterTargets",
        ]
        Resource = [
          "*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "sns:Publish",
          "elasticloadbalancing:DescribeTargetHealth",
          "elasticloadbalancing:DescribeTargetGroups",
        ]
        Resource = "*"
      },
      {
        Effect = "Allow",
        Action = [
          "ssm:GetParameter",
        ],
        Resource = "arn:aws:ssm:*:*:parameter/dbi-shared/*"
      }
    ]
  })
}
resource "aws_iam_policy_attachment" "instance_role_policy_attachment" {
  name       = "${var.resource_name_prefix}-inst-task-policy-attachment"
  roles      = [aws_iam_role.services_instance_role.name]
  policy_arn = aws_iam_policy.basic_instance_policy.arn
}
# Enables SSM access to the EC2 instances.
data "aws_iam_policy" "ssm_policy" {
  arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}
resource "aws_iam_policy_attachment" "services_ssm_policy_attachment" {
  name       = "${var.resource_name_prefix}-inst-ssm-policy-attachment"
  roles      = [aws_iam_role.services_instance_role.name]
  policy_arn = data.aws_iam_policy.ssm_policy.arn
}
resource "aws_iam_instance_profile" "services_instance_profile" {
  name = "${var.resource_name_prefix}-svc-instance-profile"
  role = aws_iam_role.services_instance_role.name
}
# -- End: EC2 Instance IAM Role and Profile

# -- Begin: Lambda Function IAM Role
# Lambda function definitions for various events
# We only define the functions here and the actual subscribing or triggering should be done
# at the place where the event is generated.

resource "aws_iam_role" "lambda_func_role" {
  name = "${var.resource_name_prefix}-role"
  assume_role_policy = jsonencode(
    {
      Version = "2012-10-17"
      Statement = [
        {
          Effect = "Allow"
          Principal = {
            Service = "lambda.amazonaws.com"
          }
          Action = "sts:AssumeRole"
        }
      ]
    }
  )
}

resource "aws_iam_role_policy" "lambda_policy" {
  name = "${var.resource_name_prefix}-lambda_policy"
  role = aws_iam_role.lambda_func_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue",
        ]
        Resource = [
          "arn:aws:secretsmanager:*:${data.aws_caller_identity.current.account_id}:*",
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "ec2:DescribeVolumes",
          "ecs:DescribeServices",
        ],
        Resource = [
          "*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "ecs:RunTask",
        ],
        Resource = [
          "arn:aws:ecs:*:${data.aws_caller_identity.current.account_id}:task-definition/*",
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*"
      },
      # Allows the Lambda function to pass roles to any ECS tasks
      {
        Effect = "Allow"
        Action = [
          "iam:PassRole"
        ]
        Resource = [
          "*"
        ]
        Condition = {
          StringEquals = {
            "iam:PassedToService" = "ecs-tasks.amazonaws.com"
          }
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_funcpolicy_attachment" {
  role       = aws_iam_role.lambda_func_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# -- End: Lambda Function IAM Role

# -- output
output "instance_iam_role_arn" {
  value = aws_iam_role.services_instance_role.arn
}
output "instance_profile_name" {
  value = aws_iam_instance_profile.services_instance_profile.name
}
output "lambda_role_arn" {
  value = aws_iam_role.lambda_func_role.arn
}
# -- This is for reference purpose. Output *all* resource ARNs for this Module.
output "all_resource_ids" {
  value = {
    instance_profile_arn = aws_iam_instance_profile.services_instance_profile.arn
    role_arn             = aws_iam_role.services_instance_role.arn
    policy_arn           = aws_iam_policy.basic_instance_policy.arn
  }
}
