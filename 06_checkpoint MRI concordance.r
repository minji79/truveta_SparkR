## 365-day pre-index

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
