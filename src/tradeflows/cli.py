import argparse
from tradeflows_cli.comtrade_datafetch import run_comtrade_download
from tradeflows_cli.census_datafetch import run_tariff_download

# helper for dealing with boolean values in CLI
def str_to_bool(value: str) -> bool:
    """
    Converts boolean-ish strings to boolean.
    """
    v = value.strip().lower()
    if v in {"true", "1", "yes", "y"}:
        return True
    if v in {"false", "0", "no", "n"}:
        return False
    raise argparse.ArgumentTypeError("Use true/false")


def main() -> None:

    parser = argparse.ArgumentParser(prog="tradeflows")
    sub = parser.add_subparsers(dest="cmd", required=True)

    # ------------------
    # Comtrade 
    # ------------------
    comtrade = sub.add_parser("download_comtrade", help="Download data from UN Comtrade")
    comtrade.add_argument("--iso3", required=True, help="Comma-separated ISO3 codes, e.g. GBR, USA")
    comtrade.add_argument("--start", required=True, help="Start period, e.g. 2025-08")
    comtrade.add_argument("--end", required=True, help="End period, e.g. 2025-10")
    comtrade.add_argument("--dataset", default="Tariffline", choices=["Tariffline", "Final"])
    comtrade.add_argument("--freq", default="M", choices=["M", "A"])
    comtrade.add_argument("--type", default="C")
    comtrade.add_argument("--cl", default="HS")
    comtrade.add_argument("--decompress", type=str_to_bool, default=True, help="true/false")
    comtrade.add_argument("--overwrite", type=str_to_bool, default=False, help="true/false")

    # ------------------
    # Census 
    # ------------------
    census = sub.add_parser("download_census", help="Download HS tariff estimates from US Census API")
    census.add_argument("--start", required=True, help="Start date YYYY-MM")
    census.add_argument("--end", required=True, help="End date YYYY-MM")
    census.add_argument("--granularity", type=int, default=10, choices=[2, 4, 6, 8, 10])
    census.add_argument("--overwrite", type=str_to_bool, default=False, help="true/false")
    # manual codes if desired
    census.add_argument("--codes", default=None, help="Comma-separated hs codes. Default is to use the handover parquet created by the comtrade download.")
    # handover selectors (when --codes not supplied)
    census.add_argument("--handover-dataset", default="Tariffline", choices=["Tariffline", "Final"])
    census.add_argument("--handover-cl", default="HS")
    census.add_argument("--handover-freq", default="M", choices=["M", "A"])


    args = parser.parse_args()

    if args.cmd == "download_comtrade":

        iso3_codes = [s.strip() for s in args.iso3.split(",") if s.strip()]

        run_comtrade_download(
            iso3_codes=iso3_codes,
            start=args.start,
            end=args.end,
            dataset=args.dataset,
            freqCode=args.freq,
            typeCode=args.type,
            clCode=args.cl,
            decompress=args.decompress,
            overwrite=args.overwrite,
        )

    if args.cmd == "download_census":
        cmdCodes = None
        if args.codes:
            cmdCodes = [s.strip() for s in args.codes.split() if s.strip()]

        run_tariff_download(
            start_date=args.start,
            end_date=args.end,
            granularity=args.granularity,
            overwrite=args.overwrite,
            cmdCodes = cmdCodes,
            handover_dataset=args.handover_dataset,
            handover_clCode=args.handover_cl,
            handover_freqCode=args.handover_freq,

        )
