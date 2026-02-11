# globals.r -------------------------------------------------------------------
#
# Suppress R CMD check notes about global variables used in NSE contexts
# (dplyr, dbplyr, tidyverse functions with unquoted column names)
#
# These are column names from OSF Parquet database tables and are not
# actual global variables.
# ------------------------------------------------------------------------------

utils::globalVariables(c(
  # Column names from osf_abstractnode
  "id",
  "type",
  "created",
  "deleted",
  "registered_meta",
  "provider_id",
  "creator_id",
  "is_public",
  "registered_date",
  "moderation_state",
  "spam_status",

  # Column names from osf_guid
  "content_type_id",
  "object_id",
  "guid_id",

  # Column names from osf_guidmetadatarecord
  "funding_info",
  "resource_type_general",
  "funder_name",
  "funder_identifier",

  # Column names from osf_abstractnode_affiliated_institutions
  "abstractnode_id",
  "institution_id",

  # Column names from osf_institution
  "name",

  # Column names from osf_registrationschema
  "_id",
  "schema_version",
  "active",
  "visible",

  # Column names from osf_abstractprovider
  "provider_id",

  # Column names from osf_subject
  "subject_id",
  "text",
  "parent_id",

  # Column names from osf_abstractnode_subjects
  # (covered above)

  # Column names from osf_nodelog
  "action",
  "params",
  "node_id",

  # Column names from osf_outcomeartifact
  "artifact_id",
  "artifact_type",
  "artifact_created",
  "artifact_deleted",
  "finalized",

  # Computed/derived column names
  "record_id",
  "funder",
  "institution",
  "template",
  "funded",
  "affiliated",
  "provider",
  "subject",
  "subject_parent",
  "template_id",

  # Intermediate calculation columns
  "linked_outputs",
  "linked_outcomes",
  "removed_outputs",
  "removed_outcomes",
  "total_resources",
  "net_resources",
  "net_outputs",
  "net_outcomes",
  "is_los",
  "los_lag",
  "los_change",

  # Summary/aggregation columns
  "n_osr",
  "have_resources",
  "have_outputs",
  "have_outcomes",
  "are_los",
  "prop_osr",
  "prop_resources",
  "prop_outputs",
  "prop_outcomes",
  "prop_los",

  # Date column
  "date"
))
