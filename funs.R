library(Rcpp)
cppFunction('NumericVector count_srp(CharacterVector sessions, CharacterVector events, CharacterVector pages) {
  NumericVector srp(sessions.size());
  String current_session = sessions[0];
  String current_page = pages[0];
  srp[0] = 1;
  for (int i = 1; i < sessions.size(); i++) {
    if (events[i] == "searchResultPage") {
      if (sessions[i] == current_session) {
        srp[i] = srp[i-1] + 1;
      } else {
        srp[i] = 1;
        current_session = sessions[i];
        current_page = pages[i];
      }
    } else {
      srp[i] = srp[i-1];
    }
  }
  return srp;
}')

extract_offset <- function(action, extra_params) {
  offset <- as.integer(NA)
  offset[action == "searchResultPage"] <- purrr::map_int(
    extra_params[action == "searchResultPage"],
    ~ ifelse(grepl("offset", .x, fixed = TRUE), jsonlite::fromJSON(.x)$offset, NA)
  )
  return(offset)
}

process_session <- function(df) {
  processed_session <- unsplit(lapply(split(df, df$srp_counter), function(df) {
    if (is.na(df$extras[1]) || df$extras[1] == "") {
      visited_pages <- rep(as.character(NA), times = nrow(df))
    } else {
      from_json <- jsonlite::fromJSON(df$extras[1], simplifyVector = FALSE)
      if (!("teamDraft" %in% names(from_json)) || all(is.na(df$article_id))) {
        visited_pages <- rep(as.character(NA), times = nrow(df))
      } else {
        team_a <- unlist(from_json$teamDraft$a)
        team_b <- unlist(from_json$teamDraft$b)
        visited_pages <- vapply(df$article_id, function(article_id) {
          if (article_id %in% team_a) {
            return("A")
          } else if (article_id %in% team_b) {
            return("B")
          } else {
            return(as.character(NA))
          }
        }, "")
      }
    }
    return(visited_pages)
  }), df$srp_counter)
  return(processed_session)
}
