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
    typeCode = "C",
    clCode = "HS",
    freqCode = "M"
) {
    file.path(
        "data",
        "comtrade",
        "parquet",
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
dbExecute(
    con,
    sprintf(
        "CREATE OR REPLACE VIEW comtrade_all AS
        SELECT * FROM read_parquet('%s', hive_partitioning=1)",
        parquet_glob
    )
)
# save as db object
df <- tbl(con, "comtrade_all")

# disconnect
dbDisconnect(con, shutdown = TRUE)


