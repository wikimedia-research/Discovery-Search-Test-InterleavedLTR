if (!dir.exists("data")) {
  dir.create("data", recursive = TRUE)
}

if (!grepl("^stat1", Sys.info()["nodename"])) {
  message("Creating an auto-closing SSH tunnel in the background...")
  # See https://gist.github.com/scy/6781836 for more info.
  system("ssh -f -o ExitOnForwardFailure=yes stat1003.eqiad.wmnet -L 3307:analytics-store.eqiad.wmnet:3306 sleep 10")
  library(RMySQL)
  con <- dbConnect(MySQL(), host = "127.0.0.1", group = "client", dbname = "log", port = 3307)
} else {
  con <- wmf::mysql_connect("log")
}

library(glue)
library(magrittr)

start_date <- as.Date("2017-08-07") + 1
end_date <- as.Date("2017-08-31") - 1

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
    date <- as.Date("2017-08-21") # TODO: remove
    message("Fetching events from ", format(date, "%d %B %Y"), "...")
    yyyymmdd <- format(date, "%Y%m%d")
    query <- glue(query)
    result <- wmf::mysql_read(query, "log", con)
    result$ts %<>% lubridate::ymd_hms()
    result$date <- as.Date(result$ts)
    # De-duplicate events:
    result <- result[order(result$session_id, result$event_id, result$ts), ]
    result <- result[!duplicated(result[, c("session_id", "event_id")]), ]
    # Remove unnecessary check-ins:
    result <- result[order(result$session_id, result$page_id, result$article_id, result$checkin, na.last = FALSE), ]
    result <- result[!duplicated(result[, c("session_id", "page_id", "article_id", "event")], fromLast = TRUE), ]
    result <- result[order(result$session_id, result$ts), ]
    # TODO: De-duplicate SERPs by linking with query hashes:
    # TODO: Extract team draft data so we know which visited result is which:
    # TODO: Remove orphan clicks (clicks without a SERP parent event):
    return(result)
  }
))

wmf::mysql_close(con)

# Interleaved testing:
interleaved <- results[group %in% c("ltr-i-20", "ltr-i-1024", "ltr-i-20-1024") & event != "click", ]
# Traditional A/B testing:
page_visits <- results[group %in% c("control", "ltr-20", "ltr-1024") & event %in% c("visitPage", "checkin"), ]
serp_clicks <- results[group %in% c("control", "ltr-20", "ltr-1024") & event %in% c("searchResultPage", "click"), ]
