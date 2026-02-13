################################################################################
# Glue Module - Outputs
################################################################################

output "simple_job_name" {
  description = "Name of the simple ETL Glue job (without data model)"
  value       = aws_glue_job.etl_simple.name
}

output "data_model_job_name" {
  description = "Name of the data model ETL Glue job (with star schema)"
  value       = aws_glue_job.etl_data_model.name
}
