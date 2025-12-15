# We always extract the AWS accountID and region from the customer's database information.
# No matter where the customer applications that consume Springtail services reside.
provider "aws" {
  region = local.dbi_region
  default_tags {
    tags = {
      OrganizationID     = var.organization_id
      AccountID          = var.account_id
      DatabaseInstanceID = var.database_instance_id
      App                = "springtail-apps"
      Environment        = var.environment
      ManagedBy = "OpenTofu"
      # May only change the values below
      Region             = local.dbi_region
      AZID               = local.az_id
      Shard              = local.dbi_shard
      TemplateVersion    = var.template_version
    }
  }
}
