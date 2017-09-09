if (!dir.exists(file.path("data", "daily"))) {
  dir.create(file.path("data", "daily"), recursive = TRUE)
}

if (!grepl("^stat1", Sys.info()["nodename"])) {
  message("Creating an auto-closing SSH tunnel in the background...")
  # See https://gist.github.com/scy/6781836 for more info.
  system("ssh -f -o ExitOnForwardFailure=yes stat1006.eqiad.wmnet -L 3307:analytics-store.eqiad.wmnet:3306 sleep 10")
  # Alternatively: `ssh -N stat6 -L 3307:analytics-store.eqiad.wmnet:3306`
  library(RMySQL)
  con <- dbConnect(MySQL(), host = "127.0.0.1", group = "client", dbname = "log", port = 3307)
} else {
  con <- wmf::mysql_connect("log")
}

library(glue)
library(magrittr)
source("funs.R")

start_date <- as.Date("2017-08-07") + 1
end_date <- as.Date("2017-08-30") - 1
# start_date <- end_date - 2 # for dev

query <- "SELECT
  timestamp AS ts,
  event_subTest AS group_id,
  event_uniqueId AS event_id,
  event_searchSessionId AS session_id,
  event_pageViewId AS page_id,
  event_hitsReturned AS hits_returned,
  event_msToDisplayResults AS load_time,
  event_action AS event,
  event_position AS position,
  event_checkin AS checkin,
  event_articleId AS article_id,
  event_extraParams AS extras,
  MD5(LOWER(TRIM(event_query))) AS query_hash,
  event_searchToken AS search_token
FROM TestSearchSatisfaction2_16909631
WHERE
  LEFT(timestamp, 8) = '{yyyymmdd}'
  AND event_source = 'fulltext'
  AND (event_subTest = 'control' OR event_subTest RLIKE '^ltr')
  AND event_action IN('searchResultPage', 'click', 'visitPage', 'checkin')
  AND INSTR(userAgent, '\"is_bot\": false') > 0"

results <- data.table::rbindlist(lapply(
  seq(start_date, end_date, "day"),
  function(date) {

    message("Fetching events from ", format(date, "%d %B %Y"), "...")
    yyyymmdd <- format(date, "%Y%m%d")
    query <- glue(query)
    result <- wmf::mysql_read(query, "log", con)
    result$ts %<>% lubridate::ymd_hms()
    result$date <- as.Date(result$ts)

    message("De-duplicating events")
    result <- result[order(result$session_id, result$event_id, result$ts), ]
    result <- result[!duplicated(result[, c("session_id", "event_id")]), ]

    message("Removing unnecessary check-ins")
    result <- result[order(result$session_id, result$page_id, result$article_id, result$checkin, na.last = FALSE), ]
    result <- result[!duplicated(result[, c("session_id", "page_id", "article_id", "event")], fromLast = TRUE), ]
    result <- result[order(result$session_id, result$ts), ]

    message("Extracting offset data from SRP events")
    result$offset <- extract_offset(result$event, result$extras)

    message("Removing orphan clicks (clicks without a SRP parent event)")
    orphans <- result %>%
      dplyr::filter(event %in% c("searchResultPage", "click")) %>%
      dplyr::arrange(session_id, page_id) %>%
      dplyr::group_by(session_id, page_id) %>%
      dplyr::summarize(has_srp = "searchResultPage" %in% event) %>%
      dplyr::ungroup() %>%
      dplyr::filter(!has_srp)
    result %<>% dplyr::anti_join(orphans, by = c("session_id", "page_id"))

    message("De-duplicating SRPs by linking with query hashes")
    srps <- result[result$event == "searchResultPage", c("group_id", "session_id", "page_id", "query_hash")]
    srps <- srps[order(srps$group_id, srps$session_id, srps$query_hash, srps$page_id), ]
    srps %<>%
      dplyr::group_by(group_id, session_id, query_hash) %>%
      dplyr::mutate(search_id = page_id[1]) %>%
      dplyr::ungroup() %>%
      dplyr::select(-query_hash)
    result %<>%
      dplyr::left_join(srps, by = c("group_id", "session_id", "page_id")) %>%
      dplyr::arrange(group_id, session_id, ts)
    result$search_id <- fill_in(result$search_id)

    message("Performing additional data processing")
    result$hits_returned[is.na(result$hits_returned) & result$event == "searchResultPage"] <- 0
    result %<>%
      dplyr::group_by(group_id, session_id) %>%
      dplyr::arrange(ts) %>%
      dplyr::mutate(srp_counter = cumunique(search_id)) %>%
      dplyr::ungroup() %>%
      dplyr::arrange(group_id, session_id, ts)

    message("Extracting team draft data so we know which visited result is which")
    result <- data.table::data.table(result)
    result[, team := process_session(.SD), by = c("group_id", "session_id"), .SDcols = c("srp_counter", "extras", "article_id")]
    result <- result[order(result$session_id, result$srp_counter, result$ts), ]
    result[, extras := NULL, ]

    message("Saving that day's data (", nrow(result), " rows)")
    result <- result[, union(c("date", "group_id", "session_id", "ts"), names(result)), with = FALSE]
    data.table::fwrite(result, file.path("data", "daily", paste0("events_", yyyymmdd, ".tsv")))

    # Output:
    return(result)
  }
))

wmf::mysql_close(con)

# Full dataset:
message("Writing full dataset (", nrow(results), " rows) out...")
data.table::fwrite(results, file.path("data", paste0("full-events_", format(start_date, "%Y%m%d"), "-", format(end_date, "%Y%m%d"), ".csv")))
readr::write_rds(results, file.path("data", paste0("full-events_", format(start_date, "%Y%m%d"), "-", format(end_date, "%Y%m%d"), ".rds")), compress = "gz")

# Interleaved testing:
interleaved <- results[results$group %in% c("ltr-i-20", "ltr-i-1024", "ltr-i-20-1024") & results$event != "click", ]
interleaved$position <- NULL
message("Writing interleaved subset (", nrow(interleaved), " rows) out...")
data.table::fwrite(interleaved, file.path("data", paste0("interleaved_", format(start_date, "%Y%m%d"), "-", format(end_date, "%Y%m%d"), ".csv")))
readr::write_rds(interleaved, file.path("data", paste0("interleaved_", format(start_date, "%Y%m%d"), "-", format(end_date, "%Y%m%d"), ".rds")), compress = "gz")

# Traditional A/B testing:
page_visits <- results[results$group %in% c("control", "ltr-20", "ltr-1024") & results$event %in% c("visitPage", "checkin"), ]
page_visits$team <- NULL
message("Writing page visit subset (", nrow(page_visits), " rows) out...")
data.table::fwrite(page_visits, file.path("data", paste0("page-visits_", format(start_date, "%Y%m%d"), "-", format(end_date, "%Y%m%d"), ".csv")))
readr::write_rds(page_visits, file.path("data", paste0("page-visits_", format(start_date, "%Y%m%d"), "-", format(end_date, "%Y%m%d"), ".rds")), compress = "gz")
serp_clicks <- results[results$group %in% c("control", "ltr-20", "ltr-1024") & results$event %in% c("searchResultPage", "click"), ]
serp_clicks$team <- NULL
message("Writing SRP/clicks subset (", nrow(serp_clicks), " rows) out...")
data.table::fwrite(serp_clicks, file.path("data", paste0("serp-clicks_", format(start_date, "%Y%m%d"), "-", format(end_date, "%Y%m%d"), ".csv")))
readr::write_rds(serp_clicks, file.path("data", paste0("serp-clicks_", format(start_date, "%Y%m%d"), "-", format(end_date, "%Y%m%d"), ".rds")), compress = "gz")
message("Done!")
