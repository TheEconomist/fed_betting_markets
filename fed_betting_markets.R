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
kget <- function(path, q=list()) {
  r <- GET(paste0(K_BASE, path), query=q); stop_for_status(r)
  fromJSON(content(r, as="text", encoding="UTF-8"), simplifyVector=FALSE)
}
kalshi_hourly <- function(ticker, start, end) {
  `%||%` <- function(a,b) if (is.null(a)) b else a
  floor_hour <- function(x) as.POSIXct(floor(as.numeric(as.POSIXct(x, tz="UTC"))/3600)*3600,
                                       origin="1970-01-01", tz="UTC")
  empty <- function() data.table(time_utc=as.POSIXct(character(), tz="UTC"),
                                 kalshi_yes_bid=numeric(),
                                 kalshi_yes_ask=numeric(),
                                 kalshi_mid     =numeric())

  K_BASE <- "https://api.elections.kalshi.com/trade-api/v2"
  kget <- function(path, q=list()) {
    r <- httr::GET(paste0(K_BASE, path), query=q); httr::stop_for_status(r)
    jsonlite::fromJSON(httr::content(r, as="text", encoding="UTF-8"), simplifyVector=FALSE)
  }

  st_global <- floor_hour(start); et_global <- floor_hour(end) + 3600
  if (st_global >= et_global) return(empty())

  # resolve series + market
  mk <- kget(paste0("/markets/", utils::URLencode(toupper(ticker))))$market
  ev <- kget(paste0("/events/",  mk$event_ticker))$event

  # fetch in 14-day chunks, nudging start by 1h on HTTP 400
  out <- list()
  chunk <- 14L * 24L * 3600L
  for (st in seq(st_global, et_global - 1, by = chunk)) {
    e_chunk <- min(st + chunk, et_global)
    attempt <- st
    x <- NULL
    while (attempt < e_chunk) {
      res <- tryCatch(
        kget(sprintf("/series/%s/markets/%s/candlesticks",
                     utils::URLencode(ev$series_ticker), utils::URLencode(mk$ticker)),
             list(start_ts=as.integer(attempt), end_ts=as.integer(e_chunk), period_interval=60)),
        error = identity
      )
      if (!inherits(res, "error")) { x <- res; break }
      if (grepl("HTTP 400", conditionMessage(res), fixed = TRUE)) {
        attempt <- attempt + 3600L   # nudge 1 hour forward
      } else {
        x <- NULL; break             # non-400 → skip this chunk
      }
    }
    if (!is.null(x) && length(x$candlesticks)) {
      dt <- data.table::rbindlist(lapply(x$candlesticks, function(c) data.table::data.table(
        time_utc       = as.POSIXct(c$end_period_ts, origin="1970-01-01", tz="UTC"),
        kalshi_yes_bid = (c$yes_bid$close %||% NA_real_)/100,
        kalshi_yes_ask = (c$yes_ask$close %||% NA_real_)/100
      )), fill=TRUE)
      out[[length(out)+1L]] <- dt
    }
  }

  if (!length(out)) return(empty())
  dt <- data.table::rbindlist(out, fill=TRUE)

  # snap to the hour so merges don’t drop them
  dt[, time_utc := as.POSIXct(floor(as.numeric(time_utc)/3600)*3600,
                              origin="1970-01-01", tz="UTC")
    ][, kalshi_mid := ifelse(is.finite(kalshi_yes_bid) & is.finite(kalshi_yes_ask),
                            (kalshi_yes_bid + kalshi_yes_ask)/2, NA_real_)]
  unique(dt, by="time_utc")[order(time_utc)]
}





# ---- Polymarket hourly (mid only via prices-history) ----
GAMMA <- "https://gamma-api.polymarket.com"
CLOB  <- "https://clob.polymarket.com"
pm_token <- function(slug, side="yes") {
  r <- GET(paste0(GAMMA, "/markets/slug/", utils::URLencode(slug, reserved=TRUE))); stop_for_status(r)
  m <- fromJSON(content(r, as="text", encoding="UTF-8"), simplifyVector=FALSE)
  outs <- tolower(trimws(as.character(unlist(if (is.character(m$outcomes) && grepl("^\\s*\\[", m$outcomes)) jsonlite::fromJSON(m$outcomes) else m$outcomes))))
  ids  <- as.character(unlist(if (is.character(m$clobTokenIds) && grepl("^\\s*\\[", m$clobTokenIds)) jsonlite::fromJSON(m$clobTokenIds) else m$clobTokenIds))
  ids[[ if (length(outs)==length(ids)) match(tolower(side), outs) else if (tolower(side)=="yes") 1L else 2L ]]
}
polymarket_hourly <- function(slug, start, end) {
  # typed empty so merges never fail
  empty_pm <- function() data.table(
    time_utc = as.POSIXct(character(), tz = "UTC"),
    polymarket_mid = numeric()
  )

  # clip to your requested window (you already know real start dates)
  st <- as.POSIXct(floor(as.numeric(as.POSIXct(start, tz="UTC"))/3600)*3600,
                   origin="1970-01-01", tz="UTC")
  et <- as.POSIXct(floor(as.numeric(as.POSIXct(end,   tz="UTC"))/3600)*3600 + 3600,
                   origin="1970-01-01", tz="UTC")
  if (st >= et) return(empty_pm())

  # get token id (Yes)
  tok <- tryCatch({
    r <- GET(paste0("https://gamma-api.polymarket.com/markets/slug/",
                    utils::URLencode(slug, reserved = TRUE)))
    if (http_error(r)) return(NA_character_)
    m <- jsonlite::fromJSON(content(r, as="text", encoding="UTF-8"), simplifyVector=FALSE)
    outs <- tolower(trimws(as.character(unlist(
      if (is.character(m$outcomes) && grepl("^\\s*\\[", m$outcomes))
        jsonlite::fromJSON(m$outcomes) else m$outcomes
    ))))
    ids <- as.character(unlist(
      if (is.character(m$clobTokenIds) && grepl("^\\s*\\[", m$clobTokenIds))
        jsonlite::fromJSON(m$clobTokenIds) else m$clobTokenIds
    ))
    if (!length(ids)) NA_character_ else ids[[ if (length(outs)==length(ids)) match("yes", outs) else 1L ]]
  }, error = function(e) NA_character_)
  if (!nzchar(tok)) return(empty_pm())

  # call prices-history; on any error/400, return empty
  q <- list(market = tok, startTs = as.integer(st), endTs = as.integer(et), fidelity = 60)
  res <- tryCatch(GET("https://clob.polymarket.com/prices-history", query = q),
                  error = function(e) NULL)
  if (is.null(res) || http_error(res)) return(empty_pm())

  h <- tryCatch(jsonlite::fromJSON(content(res, as="text", encoding="UTF-8"))$history,
                error = function(e) NULL)
  if (is.null(h) || !length(h)) return(empty_pm())

  dt <- as.data.table(h)
  setnames(dt, c("t","p"), c("time_utc","polymarket_mid"))
  dt[, time_utc := as.POSIXct(time_utc, origin="1970-01-01", tz="UTC")]
  dt[, polymarket_mid := as.numeric(polymarket_mid)]   # already 0–1
  # one row per hour (keep last within the hour)
  dt[, hr := as.POSIXct(floor(as.numeric(time_utc)/3600)*3600, origin="1970-01-01", tz="UTC")
     ][order(time_utc), .SD[.N], by=hr
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
