class()

# in lazy dataset
# display_df(), colnames(), sparklyr::sdf_nrow(lecanemab_user_apoe4)

################################################
#  load_filtered_table | when you first load the dataset, it is in R data.frame ("tbl_df" "tbl"  "data.frame")
################################################
# when you 
lecanemab_administration_all <- load_filtered_table(con, snapshot, 'MedicationAdministration',
    lecanemab_codes, view_name='lecanemab_administration_all', apply_annulled_filter=TRUE)

# in lazy table, names() is
#   colnames(lecanemab_user_rural)


################################################################################################
#  
#  when dataset is too large you need to convert all dataset into SparkR DataFrame for SQL merge
#
################################################################################################

################################################
#  sdf_register | convert Spark lazy table into SparkR DataFrame for SQL merge
################################################
# convert Lazy table into spark dataframe for SQL merge
comorb_alzheimer_spark_df <- sdf_register(comorb_alzheimer, "comorb_alzheimer_tbl")
create_view(comorb_alzheimer_spark_df, view_name="PersonListOrders")


################################################
#  convert R dataframe into SparkR DataFrame for SQL merge
#     r_df_to_spark_df
#     create_view(comorb_alzheimer_spark_df, view_name="PersonListOrders")
################################################
#  convert R dataframe into spark dataframe for SQL merge
comorb_alzheimer_spark_df <- r_df_to_spark_df(con, comorb_alzheimer, output_mode="sparkr") 

create_view(comorb_alzheimer_spark_df, view_name="PersonListOrders")



################################################
#  merging pattern
################################################

comorb_alzheimer_spark_df <- sdf_register(comorb_alzheimer, "comorb_alzheimer_tbl")
lecanemab_user_spark_df <- sdf_register(lecanemab_user, "lecanemab_user_tbl")

%%sql
CREATE OR REPLACE TEMP VIEW lecanemab_user_with_alzheimer AS
SELECT
  u.*,
  COALESCE(c.alzheimer, 0)
FROM lecanemab_user u
LEFT JOIN comorb_alzheimer c
  ON u.PersonId = c.PersonId;

sql <- "SELECT * FROM lecanemab_user_with_alzheimer"

lecanemab_user <- load_sql_table(con, snapshot, sql)  # to convert to lazy table 
lecanemab_user <- as.data.frame(load_sql_table(con, snapshot, sql))     # to convert to data frame


################################################
#  to plot figure you need to convert dataset to local R table
################################################



################################################################################################
#  
#  when dataset is too large you need to convert all dataset into SparkR DataFrame for SQL merge
#
################################################################################################

disease_codes <- dementia_codes

# Use load_filtered_table() to find EHR events that reference diagnosis
disease <- load_filtered_table(con, snapshot, 'Condition', apply_annulled_filter = TRUE,
   disease_codes, view_name = 'tbl_dementia')

disease <- disease %>% mutate(RecordedDateTime = to_date(RecordedDateTime))
disease <- disease %>% select(PersonId, RecordedDateTime)

# Merge with study_cohorts in Spark lazy table format
lecanemab_user_disease <- lecanemab_user_id %>% left_join(disease, by = "PersonId")

# screaning within 1 year look back 
lecanemab_user_disease <- lecanemab_user_disease %>%
  mutate(start_date = to_date(start_date)) %>%   # make sure start_date is in the format for lazy table
  mutate(
    in_lookback =
      RecordedDateTime >= date_sub(start_date, 365L) &
      RecordedDateTime <  start_date
  ) %>%
  mutate(
    disease = ifelse(is.na(in_lookback) | in_lookback == FALSE, 0, 1)
  ) %>% distinct()

# remain the most recent records (one recorf for one individual)
lecanemab_user_disease <- lecanemab_user_disease %>%
  group_by(PersonId) %>%
  summarise(disease = max(disease)) %>%
  ungroup() %>%
  mutate(disease = coalesce(disease, 0L))

# convert Lazy table into spark dataframe for SQL merge
lecanemab_user_disease_spark_df <- sdf_register(lecanemab_user_disease, "lecanemab_user_disease_tbl")
lecanemab_user_spark_df <- sdf_register(lecanemab_user, "lecanemab_user_tbl")


%%sql
CREATE OR REPLACE TEMP VIEW lecanemab_user_with_dementia AS
SELECT
  u.*,
  COALESCE(c.disease, 0) as dementia
FROM lecanemab_user_tbl u
LEFT JOIN lecanemab_user_disease_tbl c
  ON u.PersonId = c.PersonId;

sql <- "SELECT * FROM lecanemab_user_with_dementia"
lecanemab_user <- load_sql_table(con, snapshot, sql)






















