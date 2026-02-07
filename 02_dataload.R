##############################
# Load Comtrade using DUCKDB #
##############################
# By: DDR

# prepare workspace
library(dplyr)
library(duckdb)
library(DBI)
library(dbplyr)

# deal with confilcts
conflicted::conflicts_prefer(dplyr::filter)

# Generate parquet path
generate_parquet_path <- function(
    dataset = "Tariffline",
    typeCode = "C",
    clCode = "HS",
    freqCode = "M"
) {
    file.path(
        "data",
        "comtrade",
        "parquet",
        paste0("dataset=", dataset),
        paste0("type=", typeCode),
        paste0("cl=", clCode),
        paste0("freq=", freqCode)
    )
}

# set paths
db_path <- "data/comtrade/comtrade.duckdb"
dir.create(dirname(db_path), recursive = TRUE, showWarnings = FALSE)
parquet_glob <- file.path(generate_parquet_path(), "**", "*.parquet")

# establish duckdb connection
con <- dbConnect(duckdb(), dbdir = db_path)
# get data
tmp <- dbExecute(
    con,
    sprintf(
        "CREATE OR REPLACE VIEW comtrade_all AS
        SELECT * FROM read_parquet('%s', hive_partitioning=1)",
        parquet_glob
    )
)

q <- tbl(con, "comtrade_all") |>
  filter(flowCategory == "M") |>
  group_by(reporterCode) |>
  summarise(
    n_row  = n(),
    n_part = n_distinct(partnerCode),
    no_dig = mean(sql("length(CAST(cmdCode AS VARCHAR))")),
    .groups = "drop"
  )

show_query(q)
df <- collect(q)


# disconnect
dbDisconnect(con, shutdown = TRUE)


