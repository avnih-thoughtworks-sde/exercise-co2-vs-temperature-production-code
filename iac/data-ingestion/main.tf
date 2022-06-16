provider "aws" {}

data "aws_caller_identity" "current" {}

module "glue-job-ingestion" {
  source = "github.com/data-derp/terraform-modules//glue-job"

  project-name = var.project-name
  module-name = var.module-name
  submodule-name = "data-ingestion"
  script-path = "data-ingestion/main.py"
  additional-params = {
    "--extra-py-files":                   "s3://${var.project-name}-${var.module-name}/data-ingestion/data_ingestion-0.1-py3.egg", # https://docs.aws.amazon.com/glue/latest/dg/reduced-start-times-spark-etl-jobs.html
    "--temperatures_country_input_path":  "s3://${var.project-name}-${var.module-name}/data-source/TemperaturesByCountry.csv",
    "--temperatures_country_output_path": "s3://${var.project-name}-${var.module-name}/data-ingestion/TemperaturesByCountry.parquet",
    "--temperatures_global_input_path":   "s3://${var.project-name}-${var.module-name}/data-source/GlobalTemperatures.csv",
    "--temperatures_global_output_path":  "s3://${var.project-name}-${var.module-name}/data-ingestion/GlobalTemperatures.parquet",
    "--co2_input_path":                   "s3://${var.project-name}-${var.module-name}/data-source/EmissionsByCountry.csv",
    "--co2_output_path":                  "s3://${var.project-name}-${var.module-name}/data-ingestion/EmissionsByCountry.parquet",
  }
}