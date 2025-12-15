# A Lamabd launched into the transit VPC, triggered by CloudWatch Event
# to run a Python script periodically that resolves the IP address from the the given primary database host.
# then register the IP Target for the inbound Target Group ARN of the NLB.
# This module also creates an SNS topic specifically for the corresponding database instance.

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# Create an SNS topic for this speficic database instance
# This only existing for Private DB instances for events routing between
# transit VPCs and workload VPCs
resource "aws_sns_topic" "dbi_sns_topic" {
  name = "${var.resource_name_prefix}-sns-topic"
}


# Role granting the Lambda function the permission to update:
#   1. Publish notification to the SNS topic
#   2. Create or Update the IP Target for the input Target Group ARN of the NLB
resource "aws_iam_role" "watcher_func_role" {
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
  role = aws_iam_role.watcher_func_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "sns:Publish",
          "elasticloadbalancing:RegisterTargets",
          "elasticloadbalancing:DeregisterTargets",
        ]
        Resource = [
          aws_sns_topic.dbi_sns_topic.arn,
          var.target_group_arn,
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
        Effect = "Allow"
        Action = [
          "ec2:CreateNetworkInterface",
          "ec2:DescribeNetworkInterfaces",
          "ec2:DeleteNetworkInterface",
          "ec2:AssignPrivateIpAddresses",
          "ec2:UnassignPrivateIpAddresses"
        ]
        Resource = [
          "*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "watcher_func_policy_attachment" {
  role       = aws_iam_role.watcher_func_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# There are intermittent issues with the IAM role not being ready when the Lambda function creation has started.
# This resource is to wait for the IAM role to be ready before creating the Lambda function.
resource "time_sleep" "wait_for_iam_role" {
  depends_on      = [aws_iam_role_policy_attachment.watcher_func_policy_attachment]
  create_duration = "10s"
}

resource "null_resource" "wait_for_iam_propagation" {
  triggers = {
    role_arn   = aws_iam_role.watcher_func_role.arn
    role_hash  = sha1(jsonencode(aws_iam_role_policy.lambda_policy.policy))
    attach_set = tostring(aws_iam_role_policy_attachment.watcher_func_policy_attachment.id)
  }

  provisioner "local-exec" {
    # POSIX‑shell implementation
    interpreter = ["/bin/sh", "-c"]

    command = <<-EOS
      set -e

      echo "Waiting for IAM policy propagation on ${aws_iam_role.watcher_func_role.arn}"

      i=0
      while [ $i -lt 12 ]; do
        allowed="$(aws iam simulate-principal-policy \
                      --policy-source-arn "${aws_iam_role.watcher_func_role.arn}" \
                      --action-names logs:CreateLogGroup \
                      --query 'EvaluationResults[0].EvalDecision' \
                      --output text)"

        if [ "$allowed" = "allowed" ]; then
          echo "IAM propagation complete after $((i * 5)) seconds"
          exit 0
        fi

        i=$((i + 1))
        echo "not yet propagated ($i/12); sleeping 5 s"
        sleep 5
      done

      echo "Timed out waiting for IAM propagation" >&2
      exit 1
    EOS
  }
}


resource "aws_lambda_function" "watcher_function" {
  depends_on = [null_resource.wait_for_iam_propagation]

  function_name = "${var.resource_name_prefix}-watcher-lambda-func"
  # The script is included at the same path as this module
  filename         = "${path.module}/watcher.zip"
  source_code_hash = filebase64sha256("${path.module}/watcher.zip")
  role             = aws_iam_role.watcher_func_role.arn
  handler          = "watcher.lambda_handler"
  runtime          = "python3.11"
  timeout          = var.timeout
  memory_size      = 128
  vpc_config {
    subnet_ids         = var.vpc_subnet_ids
    security_group_ids = [var.vpc_sg_id]
  }
  environment {
    variables = {
      DATABASE_INSTANCE_ID = var.database_instance_id
      TARGET_GROUP_ARN     = var.target_group_arn
      HOSTNAME_TO_CHECK    = var.hostname_to_check
      SNS_TOPIC_ARN        = aws_sns_topic.dbi_sns_topic.arn
      LAMBDA_INTERVAL      = var.schedule_expression
      PORT                 = var.port
      CROSS_VPC            = var.cross_vpc
    }
  }
}

# An CloudWatch Event to trigger the function
resource "aws_cloudwatch_event_rule" "watcher_event_rule" {
  name                = "${var.resource_name_prefix}-event-rule"
  description         = "Event rule to trigger the watcher function every ${var.schedule_expression} seconds."
  schedule_expression = var.schedule_expression
}
resource "aws_cloudwatch_event_target" "watcher_event_target" {
  depends_on = [
    aws_lambda_function.watcher_function,
    aws_cloudwatch_event_rule.watcher_event_rule,
  ]
  rule = aws_cloudwatch_event_rule.watcher_event_rule.id
  arn  = aws_lambda_function.watcher_function.arn
}
# Allows the CloudWatch event to trigger the Lambda function
resource "aws_lambda_permission" "watcher_lambda_permission" {
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.watcher_function.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.watcher_event_rule.arn
}

# SPR-901 Add Error monitoring for the watcher function
resource "aws_cloudwatch_metric_alarm" "watcher_function_error_alarm" {
  alarm_name          = "${var.resource_name_prefix}-Errors-Alarm"
  alarm_description   = "Alarm for the watcher function errors."
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  statistic           = "Sum"
  period              = 60
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  dimensions = {
    FunctionName = aws_lambda_function.watcher_function.function_name
  }
  alarm_actions = [var.sns_topic_arn]
}

output "watcher_function_arn" {
  value = aws_lambda_function.watcher_function.arn
}
# The SNS topic ARN for this database instance
output "sns_topic_arn" {
  value = aws_sns_topic.dbi_sns_topic.arn
}

# -- This is for reference purpose. Output *all* resource ARNs for this Module.
output "all_resource_ids" {
  value = {
    event_rule_arn            = aws_cloudwatch_event_rule.watcher_event_rule.arn
    event_target_arn          = aws_cloudwatch_event_target.watcher_event_target.arn
    function_role_arn         = aws_iam_role.watcher_func_role.arn
    function_role_policy_name = aws_iam_role_policy.lambda_policy.name
    function_arn              = aws_lambda_function.watcher_function.arn
  }
}
