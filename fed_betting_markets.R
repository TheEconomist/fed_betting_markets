library(renv)
library(here)
library(httr)
library(jsonlite)
library(data.table)
library(ggplot2)

# ---- Known market start dates (UTC) ----
STARTS <- list(
  cook_kalshi  = as.POSIXct("2025-08-21 00:00:00", tz="UTC"),
  cook_poly    = as.POSIXct("2025-08-26 00:00:00", tz="UTC"),
  powell_kalshi= as.POSIXct("2025-01-20 00:00:00", tz="UTC"),
  powell_poly  = as.POSIXct("2025-01-30 00:00:00", tz="UTC")
)

# ---- Helpers ----
`%||%` <- function(a,b) if (is.null(a)) b else a
floor_hour <- function(x) as.POSIXct(floor(as.numeric(as.POSIXct(x, tz="UTC"))/3600)*3600,
                                     origin="1970-01-01", tz="UTC")

# ---- Kalshi hourly (uses bid/ask close; mid = (bid+ask)/2) ----
K_BASE <- "https://api.elections.kalshi.com/trade-api/v2"

kget <- function(path, q = list(), max_attempts = 6) {
  pause <- 1
  last_status <- NA_integer_
  for (i in seq_len(max_attempts)) {
    r <- httr::GET(paste0(K_BASE, path), query = q)
    s <- httr::status_code(r); last_status <- s
    if (s >= 200 && s < 300) {
      return(jsonlite::fromJSON(httr::content(r, as = "text", encoding = "UTF-8"),
                                simplifyVector = FALSE))
    }
    # retry on transient/server throttling
    if (s %in% c(429, 500, 502, 503, 504)) {
      Sys.sleep(pause); pause <- min(pause * 2, 16)
      next
    }
    # non-retriable client errors: throw
    httr::stop_for_status(r)
  }
  stop(sprintf("Kalshi GET %s failed after %d attempts (last status %s).",
               path, max_attempts, last_status))
}

kalshi_hourly <- function(ticker, start, end) {
  # typed empty so merges never fail
  empty <- function() data.table(
    time_utc        = as.POSIXct(character(), tz = "UTC"),
    kalshi_yes_bid  = numeric(),
    kalshi_yes_ask  = numeric(),
    kalshi_mid      = numeric()
  )

  # guard + hour snapping
  st_global <- floor_hour(start)
  et_global <- floor_hour(end) + 3600
  if (st_global >= et_global) return(empty())

  # resolve market (do NOT call /events/{...} resource—it's flaky)
  mk <- tryCatch(
    kget(paste0("/markets/", utils::URLencode(toupper(ticker), reserved = TRUE)))$market,
    error = function(e) NULL
  )
  if (is.null(mk)) return(empty())

  # optional: known series fallbacks for rare series-candles fallback path
  SERIES_CACHE <- list(
    "KXLEAVELISACOOK-26JAN" = "KXLEAVELISACOOK",
    "LEAVEPOWELL-25-DEC31"  = "KXLEAVEPOWELL"
  )

  # helper: convert a candlesticks payload to DT
  to_dt <- function(x) {
    if (is.null(x) || is.null(x$candlesticks) || !length(x$candlesticks)) return(NULL)
    data.table::rbindlist(lapply(x$candlesticks, function(c) data.table::data.table(
      time_utc       = as.POSIXct(c$end_period_ts, origin = "1970-01-01", tz = "UTC"),
      kalshi_yes_bid = (c$yes_bid$close %||% NA_real_) / 100,
      kalshi_yes_ask = (c$yes_ask$close %||% NA_real_) / 100
    )), fill = TRUE)
  }

  out <- list()
  chunk <- 14L * 24L * 3600L
  for (st in seq(st_global, et_global - 1, by = chunk)) {
    e_chunk <- min(st + chunk, et_global)

    # 1) Preferred: event-candlesticks (no /events resource call; just data)
    dt <- tryCatch(
      to_dt(kget(
        paste0("/events/", utils::URLencode(mk$event_ticker, reserved = TRUE), "/candlesticks"),
        q = list(start_ts = as.integer(st), end_ts = as.integer(e_chunk), period_interval = 60)
      )),
      error = function(e) NULL
    )

    # 2) Fallback: series+market candlesticks (only if we can infer series)
    if (is.null(dt)) {
      series_ticker <- SERIES_CACHE[[ mk$ticker ]] %||% NA_character_
      if (nzchar(series_ticker)) {
        dt <- tryCatch(
          to_dt(kget(
            sprintf("/series/%s/markets/%s/candlesticks",
                    utils::URLencode(series_ticker, reserved = TRUE),
                    utils::URLencode(mk$ticker,       reserved = TRUE)),
            q = list(start_ts = as.integer(st), end_ts = as.integer(e_chunk), period_interval = 60)
          )),
          error = function(e) NULL
        )
      }
    }

    if (!is.null(dt)) out[[length(out) + 1L]] <- dt
  }

  if (!length(out)) return(empty())

  dt <- data.table::rbindlist(out, fill = TRUE)
  dt[, time_utc := as.POSIXct(floor(as.numeric(time_utc) / 3600) * 3600,
                              origin = "1970-01-01", tz = "UTC")]
  dt[, kalshi_mid := ifelse(is.finite(kalshi_yes_bid) & is.finite(kalshi_yes_ask),
                            (kalshi_yes_bid + kalshi_yes_ask) / 2, NA_real_)]
  unique(dt, by = "time_utc")[order(time_utc)]
}

# ---- Polymarket hourly (mid only via prices-history) ----
GAMMA <- "https://gamma-api.polymarket.com"
CLOB  <- "https://clob.polymarket.com"
pm_token <- function(slug, side = "yes") {
  side <- tolower(side)
  # 1) fetch market by slug using the current Gamma endpoint
  r <- httr::GET("https://gamma-api.polymarket.com/markets",
                 query = list(slug = slug, limit = 1))
  if (httr::http_error(r)) return(NA_character_)
  m <- jsonlite::fromJSON(httr::content(r, as = "text", encoding = "UTF-8"),
                          simplifyVector = FALSE)
  if (!length(m)) return(NA_character_)
  m <- m[[1]]

  # 2) robustly parse outcomes + token ids (can be stringified JSON or arrays)
  parse_maybe_json <- function(x) {
    if (is.null(x)) return(list())
    if (is.character(x) && grepl("^\\s*\\[", x)) {
      # string that actually contains a JSON array
      return(as.list(jsonlite::fromJSON(x)))
    }
    if (is.list(x)) return(x)
    if (is.character(x)) return(as.list(x))
    list()
  }
  outs <- tolower(trimws(as.character(unlist(parse_maybe_json(m$outcomes)))))
  ids  <- as.character(unlist(parse_maybe_json(m$clobTokenIds)))

  if (!length(ids)) return(NA_character_)
  # 3) pick the token matching requested side ("yes"), else default to first
  if (length(outs) == length(ids) && side %in% outs) {
    ids[[ match(side, outs) ]]
  } else {
    ids[[ 1L ]]
  }
}
polymarket_hourly <- function(slug, start, end) {
  # typed empty so merges never fail
  empty_pm <- function() data.table(
    time_utc = as.POSIXct(character(), tz = "UTC"),
    polymarket_mid = numeric()
  )

  # snap to hours (UTC)
  st <- as.POSIXct(floor(as.numeric(as.POSIXct(start, tz="UTC"))/3600)*3600,
                   origin="1970-01-01", tz="UTC")
  et <- as.POSIXct(floor(as.numeric(as.POSIXct(end,   tz="UTC"))/3600)*3600 + 3600,
                   origin="1970-01-01", tz="UTC")
  if (st >= et) return(empty_pm())

  # resolve Yes token id (uses your updated pm_token())
  tok <- tryCatch(pm_token(slug, side = "yes"), error = function(e) NA_character_)
  if (!nzchar(tok)) return(empty_pm())

  # --- robust GET for a single chunk ---
  get_hist <- function(q, tries = 5) {
    pause <- 1
    for (i in seq_len(tries)) {
      res <- try(httr::GET("https://clob.polymarket.com/prices-history", query = q), silent = TRUE)
      if (inherits(res, "response")) {
        s <- httr::status_code(res)
        if (s >= 200 && s < 300) {
          return(tryCatch(jsonlite::fromJSON(httr::content(res, as="text", encoding="UTF-8"))$history,
                          error = function(e) NULL))
        }
        # transient → retry
        if (s %in% c(429, 500, 502, 503, 504)) { Sys.sleep(pause); pause <- min(pause*2, 16); next }
        # hard fail → return tag for caller to inspect
        return(structure(list(error = TRUE,
                              status = s,
                              body = try(httr::content(res, as="text", encoding="UTF-8"), silent=TRUE)),
                         class = "pm_err"))
      }
      Sys.sleep(pause); pause <- min(pause*2, 16)
    }
    NULL
  }

  # --- chunked fetch across [st, et) ---
  # start with ~21 days; adapt smaller if API complains about interval length
  chunk_secs <- 21L * 24L * 3600L
  min_chunk  <- 6L * 3600L        # never go below 6h per call
  out <- list()
  cur <- st
  while (cur < et) {
    cur_end <- min(cur + chunk_secs, et)
    q <- list(market = tok, startTs = as.integer(cur), endTs = as.integer(cur_end), fidelity = 60)
    h <- get_hist(q)

    # If server says "interval is too long", shrink chunk and retry without advancing the cursor
    if (inherits(h, "pm_err")) {
      msg <- tryCatch(jsonlite::fromJSON(h$body)$error, error = function(e) "")
      if (h$status == 400 && grepl("interval is too long", msg, ignore.case = TRUE)) {
        # halve the chunk, but not below min_chunk
        new_chunk <- max(min_chunk, as.integer(floor(chunk_secs / 2)))
        if (new_chunk == chunk_secs) {
          # already at min → skip this window to avoid infinite loop
          cur <- cur_end
        } else {
          chunk_secs <- new_chunk
        }
        next
      } else {
        # other 4xx → skip this window
        cur <- cur_end
        next
      }
    }

    if (!is.null(h) && length(h)) {
      out[[length(out)+1L]] <- data.table::as.data.table(h)
    }
    cur <- cur_end
  }

  if (!length(out)) return(empty_pm())

  dt <- data.table::rbindlist(out, fill = TRUE)
  data.table::setnames(dt, c("t","p"), c("time_utc","polymarket_mid"))
  dt[, time_utc := as.POSIXct(time_utc, origin="1970-01-01", tz="UTC")]
  dt[, polymarket_mid := as.numeric(polymarket_mid)]

  # one row per hour (keep last within the hour)
  dt[, hr := as.POSIXct(floor(as.numeric(time_utc)/3600)*3600, origin="1970-01-01", tz="UTC")
     ][order(time_utc), .SD[.N], by = hr
     ][, .(time_utc = hr, polymarket_mid)]
}


# ---- Merge for one pair, with NA before starts and avg rule ----
pair_hourly <- function(kalshi_ticker, pm_slug, start, end, k_start, p_start, suffix) {
  grid <- data.table(time_utc = seq(floor_hour(start), floor_hour(end), by="1 hour"))
  k  <- kalshi_hourly(kalshi_ticker, max(start, k_start), end)
  pm <- polymarket_hourly(pm_slug,     max(start, p_start), end)
  out <- merge(grid, k,  by="time_utc", all.x=TRUE)
  out <- merge(out,   pm, by="time_utc", all.x=TRUE)
  # fill site series forward (does NOT fill leading NAs)
  out[, c("kalshi_yes_bid","kalshi_yes_ask","kalshi_mid") :=
    lapply(.SD, data.table::nafill, type = "locf"),
    .SDcols = c("kalshi_yes_bid","kalshi_yes_ask","kalshi_mid")
    ][, polymarket_mid := data.table::nafill(polymarket_mid, type = "locf")

# average: keep the single site’s value if the other is NA; NA if both NA
    ][, avg_mid := rowMeans(cbind(kalshi_mid, polymarket_mid), na.rm = TRUE)
    ][is.nan(avg_mid), avg_mid := NA_real_
    ][, avg_mid := rowMeans(cbind(kalshi_mid, polymarket_mid), na.rm = TRUE)
    ][is.nan(avg_mid), avg_mid := NA_real_]
  setnames(out,
           c("kalshi_yes_bid","kalshi_yes_ask","kalshi_mid","polymarket_mid","avg_mid"),
           paste0(c("kalshi_yes_bid_","kalshi_yes_ask_","kalshi_mid_","polymarket_mid_","avg_mid_"), suffix))
  out
}

# ---- Public function: both pairs, one table ----
compare_two_pairs <- function(start, end) {
  A <- pair_hourly("KXLEAVELISACOOK-26JAN",
                   "lisa-cook-out-as-fed-governor-by-december-31",
                   start, end, STARTS$cook_kalshi, STARTS$cook_poly, "cook")
  B <- pair_hourly("LEAVEPOWELL-25-DEC31",
                   "will-trump-remove-jerome-powell",
                   start, end, STARTS$powell_kalshi, STARTS$powell_poly, "powell")
  merge(A, B, by="time_utc", all=TRUE)[order(time_utc)]
}

# ---- Example (Jan 20 to now), plus a quick plot of avg mids ----
dt <- compare_two_pairs(as.POSIXct("2025-01-20 00:00:00", tz="UTC"), Sys.time())
fwrite(dt, here('latest_fed_betting_market_prices.csv'))
ggplot(
 melt(dt[, .(time_utc, avg_mid_cook, avg_mid_powell)], id.vars="time_utc",
      variable.name="pair", value.name="avg_mid")
) + geom_line(aes(time_utc, avg_mid, color=pair), na.rm=TRUE) +
   labs(x="Time (UTC)", y="Average mid (Yes prob)", color=NULL) +
   theme_minimal()
ggsave(here('latest_fed_betting_market_plot.pdf'))

# Clear workspace
rm(list=ls())
gc()
