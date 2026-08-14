
# ============================================================
# 
# ============================================================
build_checkpoints <- function(infusions, index_tbl, mri_long, checkpoints, baseline_days) {
  out <- list()
  for (drug in names(checkpoints)) {
    inf <- infusions %>% filter(cohort == drug)
    idx <- index_tbl %>% filter(cohort == drug) %>% select(all_of(cohort_keys), index_date)
    mri <- mri_long  %>% filter(cohort == drug) %>% select(all_of(cohort_keys), mri_date) %>% distinct()

    for (k in checkpoints[[drug]]) {
      cur  <- inf %>% filter(infusion_number == k)      %>% select(all_of(cohort_keys), inf_k   = infusion_date)
      prev <- inf %>% filter(infusion_number == k - 1L) %>% select(all_of(cohort_keys), inf_km1 = infusion_date)

      # Denominator = patients who REACHED infusion k
      rows <- cur %>%
        left_join(prev, by = cohort_keys) %>%
        left_join(idx,  by = cohort_keys) %>%
        mutate(checkpoint = k,
               win_start = if (k == 1L) index_date - baseline_days else inf_km1,
               win_end   = if (k == 1L) index_date                else inf_k)

      # PRIMARY RULE: strictly before the upper edge for ALL checkpoints (baseline & later)
      hits <- rows %>%
        select(all_of(cohort_keys), win_start, win_end) %>%
        inner_join(mri, by = cohort_keys) %>%
        mutate(in_window = if (k == 1L) (mri_date >= win_start & mri_date < win_end)
                                        else (mri_date >  win_start & mri_date < win_end)) %>%
        filter(in_window) %>%
        group_by(across(all_of(cohort_keys))) %>%
        summarise(mri_received = 1L, n_mri_in_window = n_distinct(mri_date), .groups = "drop")

      out[[paste(drug, k, sep = "_")]] <- rows %>%
        left_join(hits, by = cohort_keys) %>%
        mutate(mri_received    = coalesce(mri_received, 0L),
               n_mri_in_window = coalesce(n_mri_in_window, 0L))
    }
  }
  bind_rows(out)
}


# ============================================================
# Link MRI events to each patient-drug episode, then build checkpoints (primary 120d)
# ============================================================

# MRI events windowed to each episode (join by PersonId; a dual-drug patient gets both)
mri_long <- index_tbl %>%
  inner_join(mri_events %>% select(PersonId, mri_date), by = "PersonId",
             relationship = "many-to-many") %>%
  filter(mri_date >= index_date - max(baseline_windows),
         mri_date <= params$analysis_endDate) %>%
  distinct(PatientId, PersonId, cohort, index_date, mri_date)

checkpoint_rows <- build_checkpoints(infusions, index_tbl, mri_long,
                                     mri_checkpoints, params$baseline_mri_days) %>%
  mutate(checkpoint_date = as.Date(win_end),
         fda_era = if_else(win_end < params$fda_lec_mri3_date, "Pre-2025 update", "Post-2025 update"),
         # Lecanemab infusion-3 MRI only applies on/after the FDA update
         applies = !(cohort == "lecanemab" & checkpoint == 3L &
                     win_end < params$fda_lec_mri3_date))

# One row per patient-drug-checkpoint?
stopifnot(nrow(checkpoint_rows %>% count(PatientId, cohort, checkpoint) %>% filter(n > 1)) == 0)
saveRDS(checkpoint_rows, file.path(artifacts_results_dir, "MRI_checkpoint_rows.rds"))


# Baseline look back 
# Rebuild ONLY the baseline checkpoint under each window. We call the same
# build_checkpoints() with a baseline-only checkpoint list, so no other logic changes.
baseline_only <- list(lecanemab = c(1), donanemab = c(1))

baseline_sweep <- purrr::map_dfr(baseline_windows, function(w) {
  build_checkpoints(infusions, index_tbl, mri_long, baseline_only, w) %>%
    group_by(cohort) %>%
    summarise(baseline_window_days = w,
              n_reached  = n(),
              n_with_mri = sum(mri_received, na.rm = TRUE),
              pct_with_mri = round(100 * mean(mri_received, na.rm = TRUE), 1),
              .groups = "drop")
}) %>%
  arrange(cohort, baseline_window_days)

print(baseline_sweep, n = Inf)



# ============================================================
# comparison figure with two bars
# ============================================================
          # Figure: baseline concordance by lookback window, grouped by drug
baseline_sweep_plot <- baseline_sweep %>%
  mutate(win_f = factor(baseline_window_days,
                        levels = baseline_windows,
                        labels = paste0(baseline_windows, "d")),
         cohort_lab = recode(cohort, lecanemab = "Lecanemab", donanemab = "Donanemab"))

p_baseline_sweep <- ggplot(baseline_sweep_plot,
                           aes(x = win_f, y = pct_with_mri, fill = cohort_lab)) +
  geom_col(position = position_dodge(width = 0.8), width = 0.7) +
  geom_text(aes(label = sprintf("%.1f%%\n(%d/%d)", pct_with_mri, n_with_mri, n_reached)),
            position = position_dodge(width = 0.8), vjust = -0.3, size = 2.8, lineheight = 0.9) +
  scale_fill_manual(values = c("Lecanemab" = "#8B6914", "Donanemab" = "#2E5A47"), name = NULL) +
  scale_y_continuous("Baseline MRI concordance (%)", limits = c(0, 100),
                     breaks = seq(0, 100, 25), expand = expansion(mult = c(0, 0.08))) +
  labs(x = "Baseline lookback window (days before index, strictly before)",
       title = "Baseline Completed MRI concordance by lookback window",
       subtitle = "Sensitivity: only the baseline checkpoint varies; 120d is primary") +
  theme_classic(base_size = 11) +
  theme(legend.position = "top", axis.title = element_text(face = "bold"),
        plot.title = element_text(face = "bold"))
print(p_baseline_sweep)
ggsave(file.path(artifacts_images_dir, "fig_baseline_window_sensitivity.png"),
       p_baseline_sweep, width = 8, height = 5, dpi = 300)



# ============================================================
# comparison figure with two bars 2
# ============================================================
    
fig_b_df_comp <- checkpoint_rows %>% filter(cohort == "lecanemab", applies) %>%
  mutate(fda_era = if_else(win_end < params$fda_lec_mri3_date, "Pre-2025", "Post-2025")) %>%
  group_by(checkpoint, fda_era) %>%
  summarise(pct = 100 * mean(mri_received), n_mri = sum(mri_received), n_reached = n(), .groups = "drop") %>%
  mutate(label = factor(c("1"="Baseline","3"="Inf 3","5"="Inf 5","7"="Inf 7","14"="Inf 14")[as.character(checkpoint)],
                        levels = c("Baseline","Inf 3","Inf 5","Inf 7","Inf 14")),
         fda_era = factor(fda_era, levels = c("Pre-2025","Post-2025")))

p_b_comp <- ggplot(fig_b_df_comp, aes(x = label, y = pct, fill = fda_era)) +
  geom_col(position = position_dodge(width = 0.8), width = 0.7) +
  geom_text(aes(label = sprintf("%.1f%%", pct)), position = position_dodge(width = 0.8), vjust = -0.4, size = 3) +
  scale_fill_manual(values = c("Pre-2025" = "#9FB8AD", "Post-2025" = "#2E5A47"), name = "FDA era") +
  scale_y_continuous("MRI concordance (%)", limits = c(0, 105), breaks = seq(0,100,25),
                     expand = expansion(mult = c(0,0.02))) +
  labs(x = "FDA-recommended MRI checkpoint",
       title = "Lecanemab MRI concordance before and after the 2025 update",
       subtitle = "Post-update period begins August 28, 2025") +
  theme_classic() + theme(legend.position = "top", axis.title = element_text(face = "bold"))
print(p_b_comp)



               
# ============================================================
# TABLE 1
# ============================================================

               library(gtsummary)
library(dplyr)

util_counts_vars <- c("n_inpatient_1yr", "n_ed_1yr", "n_outpatient_1yr")

# Display labels
var_labels <- list(
  age_at_index ~ "Age, years",
  Sex ~ "Sex",
  Race ~ "Race",
  Ethnicity ~ "Ethnicity",
  rural_urban ~ "Rural-urban",
  any_dementia ~ "Any dementia",
  alzheimer ~ "Alzheimer's disease",
  any_dementia_or_mci ~ "Any dementia or MCI",
  MildCognitiveImpairment ~ "MCI",
  apoe_tested ~ "Apolipoprotein E testing",
  any_dementia_tx ~ "Any dementia treatment",
  any_anticoagulant ~ "Any anticoagulant",
  any_antiplatelet ~ "Any antiplatelet",
  CharlsonComorbidityIndex ~ "Charlson Comorbidity Index",
  ElixhauserComorbidityScore ~ "Elixhauser Comorbidity Score",
  depression ~ "Depression",
  anxiety ~ "Anxiety",
  any_inpatient_1yr ~ "Inpatient visit, prior year",
  n_inpatient_1yr ~ "Inpatient visits, prior year, n",
  any_ed_1yr ~ "ED visit, prior year",
  n_ed_1yr ~ "ED visits, prior year, n",
  any_outpatient_1yr ~ "Outpatient visit, prior year",
  n_outpatient_1yr ~ "Outpatient visits, prior year, n"
)

tbl_vars <- c(
  "age_at_index",
  "Sex",
  "Race",
  "Ethnicity",
  "rural_urban",
  "any_dementia",
  "alzheimer",
  "any_dementia_or_mci",
  "MildCognitiveImpairment",
  "apoe_tested",
  "any_dementia_tx",
  "any_anticoagulant",
  "any_antiplatelet",
  "CharlsonComorbidityIndex",
  "ElixhauserComorbidityScore",
  "depression",
  "anxiety",
  "any_inpatient_1yr",
  "n_inpatient_1yr",
  "any_ed_1yr",
  "n_ed_1yr",
  "any_outpatient_1yr",
  "n_outpatient_1yr"
)

# Keep only variables present in the actual Table 1 dataset
tbl_vars <- intersect(tbl_vars, colnames(df_t1))

# Keep only labels for included variables
var_labels <- var_labels[
  sapply(var_labels, function(f) all.vars(f)[1] %in% tbl_vars)
]

comorbidity_indices <- intersect(
  c("CharlsonComorbidityIndex", "ElixhauserComorbidityScore"),
  tbl_vars
)

# visit-count vars actually present (guards against a missing column)
util_vars_present <- intersect(util_counts_vars, tbl_vars)

t1 <- df_t1 %>%
  select(initiation_group, all_of(tbl_vars)) %>%
  tbl_summary(
    by = initiation_group,
    label = var_labels,

    type = list(
      age_at_index ~ "continuous",
      all_of(comorbidity_indices) ~ "continuous",
      all_of(util_vars_present) ~ "continuous"
    ),

    statistic = list(
      age_at_index ~ "{median} ({p25}-{p75})",
      all_of(comorbidity_indices) ~ "{mean} ({sd})",
      all_of(util_vars_present) ~ "{median} ({p25}-{p75})",
      all_categorical() ~ "{n} ({p}%)"
    ),

    digits = list(
      age_at_index ~ 1,
      all_of(comorbidity_indices) ~ 1,
      all_of(util_vars_present) ~ 0,
      all_categorical() ~ c(0, 1)
    ),

    missing = "ifany",
    missing_text = "Unknown"
  ) %>%

  add_overall(last = TRUE) %>%

  modify_header(
    label ~ "**Characteristic**",
    all_stat_cols() ~ "**{level}**  \nN = {n}",
    stat_0 ~ "**Overall**  \nN = {N}"
  ) %>%

  modify_spanning_header(
    all_stat_cols() ~ "**Drug initiation group**"
  ) %>%

  modify_caption(
    "**Table 1. Baseline characteristics of patients initiating anti-amyloid monoclonal antibody treatment**"
  ) %>%

  modify_source_note(
    paste0(
      "Data are presented as median (IQR) for age and prior-year visit counts, ",
      "mean (SD) for the Charlson and Elixhauser comorbidity scores, ",
      "and n (%) for categorical variables."
    )
  ) %>%

  modify_source_note(
    paste0(
      "The groups are mutually exclusive. ",
      "The Both group includes patients who initiated both lecanemab and ",
      "donanemab; baseline characteristics are anchored to the patient's ",
      "earliest anti-amyloid monoclonal antibody initiation."
    )
  ) %>%

  modify_source_note(
    paste0(
      "Percentages are calculated among patients with nonmissing data; ",
      "missing values are displayed as Unknown."
    )
  ) %>%

  modify_abbreviation(
    "ED = emergency department; IQR = interquartile range; MCI = mild cognitive impairment; SD = standard deviation"
  ) %>%

  bold_labels()

t1_personlevel <- t1

t1_personlevel
         
               
# ============================================================
# TABLE — DOCUMENTED MRI SURVEILLANCE BY CHECKPOINT
# ============================================================

deliverable_table <- checkpoint_rows %>%
  filter(applies) %>%

  mutate(
    checkpoint_label =
      if_else(
        checkpoint == 1,
        "Baseline",
        paste0("Infusion ", checkpoint)
      ),

    cohort =
      recode(
        cohort,
        "lecanemab" = "Lecanemab",
        "donanemab" = "Donanemab"
      )
  ) %>%

  count(
    cohort,
    checkpoint,
    checkpoint_label,
    documentation_group,
    name = "n"
  ) %>%

  group_by(
    cohort,
    checkpoint,
    checkpoint_label
  ) %>%

  mutate(
    n_reached = sum(n),

    cell =
      sprintf(
        "%d (%.1f%%)",
        n,
        100 * n / n_reached
      )
  ) %>%

  ungroup() %>%

  select(
    cohort,
    checkpoint,
    checkpoint_label,
    n_reached,
    documentation_group,
    cell
  ) %>%

  pivot_wider(
    names_from = documentation_group,
    values_from = cell,
    values_fill = "0 (0.0%)"
  ) %>%

  arrange(
    cohort,
    checkpoint
  ) %>%

  select(
    -checkpoint
  ) %>%

  rename(
    Treatment = cohort,
    `Monitoring checkpoint` = checkpoint_label,
    `N reaching checkpoint` = n_reached
  )


knitr::kable(
  deliverable_table,
  align = "llc",
  caption = "Documented MRI surveillance at applicable monitoring checkpoints"
)
               
