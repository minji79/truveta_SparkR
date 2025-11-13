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
