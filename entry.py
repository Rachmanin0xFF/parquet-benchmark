import argparse
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(
        description="Extract Open Addresses files and put them in locations for Overture feeds"
    )
    parser.add_argument(
        "--key",
        type=str,
        required=True,
        help="An S3 object key pointing to a parquet file.",
    )
    parser.add_argument(
        "--samples",
        type=int,
        required=False,
        default=10,
    )
    parser.add_argument(
        "--seed",
        type=int,
        required=False,
        default=42,
        help="Random seed for stochastic sampling",
    )
    parser.add_argument(
        "--bbox",
        type=str,
        required=False,
        help="Comma-separated bbox coordinates: min_lon,min_lat,max_lon,max_lat",
    )
    args = parser.parse_args()

    print("yoyoyo")


if __name__ == "__main__":
    main()
