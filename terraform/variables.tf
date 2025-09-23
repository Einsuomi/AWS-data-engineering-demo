variable "environment" {
  description = "The deployment environment (dev, test, or prod)"
  type        = string
}

variable "medallion_layers" {
  description = "A list of the Medallion architecture layers"
  type        = list(string)
  default     = ["managed_uc", "landing", "bronze", "silver", "gold"]
}
