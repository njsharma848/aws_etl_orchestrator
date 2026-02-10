################################################################################
# CloudWatch Log Group for Step Functions
################################################################################

resource "aws_cloudwatch_log_group" "sfn_log_group" {
  name              = "/aws/states/${var.project_name}-${var.environment}-etl-orchestrator"
  retention_in_days = 30

  tags = var.tags
}

################################################################################
# AWS Step Functions State Machine
################################################################################

resource "aws_sfn_state_machine" "etl_orchestrator" {
  name     = "${var.project_name}-${var.environment}-etl-orchestrator"
  role_arn = var.step_functions_role_arn

  definition = jsonencode({
    Comment = "ETL Orchestrator - ${var.environment}"
    StartAt = "ProcessJobs"
    States = {
      ProcessJobs = {
        Type           = "Map"
        MaxConcurrency = 1
        ItemsPath      = "$.glue_jobs"
        Iterator = {
          StartAt = "RunGlueJob"
          States = {
            RunGlueJob = {
              Type     = "Task"
              Resource = "arn:aws:states:::glue:startJobRun.sync"
              Parameters = {
                JobName = var.glue_job_name
                Arguments = {
                  "--job_id.$"           = "$.job_id"
                  "--job_name.$"         = "$.job_name"
                  "--source_file_name.$" = "$.source_file_name"
                  "--target_table.$"     = "$.target_table"
                  "--upsert_keys.$"      = "States.JsonToString($.upsert_keys)"
                }
              }
              Catch = [
                {
                  ErrorEquals = ["States.ALL"]
                  ResultPath  = "$.error"
                  Next        = "LogError"
                }
              ]
              End = true
            }
            LogError = {
              Type   = "Pass"
              Result = "Job failed, continuing..."
              End    = true
            }
          }
        }
        End = true
      }
    }
  })

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.sfn_log_group.arn}:*"
    include_execution_data = true
    level                  = "ALL"
  }

  tags = var.tags
}
