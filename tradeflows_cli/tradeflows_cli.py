import argparse
from comtrade_datafetch import run_comtrade_download

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

    download = sub.add_parser("download_comtrade", help="Download data from UN Comtrade")
    download.add_argument("--iso3", required=True, help="Comma-separated ISO3 codes, e.g. GBR, USA")
    download.add_argument("--start", required=True, help="Start period, e.g. 2025-08")
    download.add_argument("--end", required=True, help="End period, e.g. 2025-10")
    download.add_argument("--dataset", default="Tariffline", choices=["Tariffline", "Final"])
    download.add_argument("--freq", default="M", choices=["M", "A"])
    download.add_argument("--type", default="C")
    download.add_argument("--cl", default="HS")
    download.add_argument("--decompress", type=str_to_bool, default=True, help="true/false")
    download.add_argument("--overwrite", type=str_to_bool, default=False, help="true/false")


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


