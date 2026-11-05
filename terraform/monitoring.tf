# The job exits non-zero when DQ fails (old version stays live). Alert on any failed or
# timed-out run so someone looks at _dq/<run_id>.json the same morning.
resource "aws_sns_topic" "pipeline_alerts" {
  name              = "${local.name}-pipeline-alerts"
  kms_master_key_id = "alias/aws/sns"
}

resource "aws_sns_topic_subscription" "email" {
  count     = var.alarm_email == "" ? 0 : 1
  topic_arn = aws_sns_topic.pipeline_alerts.arn
  protocol  = "email"
  endpoint  = var.alarm_email
}

resource "aws_cloudwatch_event_rule" "glue_job_failed" {
  name        = "${local.name}-scd2-job-failed"
  description = "SCD2 Glue job failed, timed out or was stopped"
  event_pattern = jsonencode({
    source      = ["aws.glue"]
    detail-type = ["Glue Job State Change"]
    detail = {
      jobName = [aws_glue_job.scd2_daily.name]
      state   = ["FAILED", "TIMEOUT", "STOPPED"]
    }
  })
}

resource "aws_cloudwatch_event_target" "to_sns" {
  rule = aws_cloudwatch_event_rule.glue_job_failed.name
  arn  = aws_sns_topic.pipeline_alerts.arn
}

data "aws_iam_policy_document" "sns_from_events" {
  statement {
    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.pipeline_alerts.arn]
    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }
  }
}

resource "aws_sns_topic_policy" "pipeline_alerts" {
  arn    = aws_sns_topic.pipeline_alerts.arn
  policy = data.aws_iam_policy_document.sns_from_events.json
}
