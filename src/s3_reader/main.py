import argparse

from .s3_reader import list_keys_by_prefixes, download_by_keys


def main():
    parser = argparse.ArgumentParser('S3 Reader')
    subparser = parser.add_subparsers(dest='func')

    parser_list = subparser.add_parser('s3list', help='Get AWS S3 files list')
    parser_list.add_argument('-b', '--bucket', help='AWS bucket name')
    parser_list.add_argument('-p', '--prefixes', nargs='+', help='AWS prefix name')

    parser_download = subparser.add_parser('s3download', help='Get AWS S3 files data')
    parser_download.add_argument('-b', '--bucket', help='AWS bucket name')
    parser_download.add_argument('-k', '--keys', nargs='+', help='AWS file key name')
    parser_download.add_argument('-f', '--output_format', help='AWS files output format')

    args = parser.parse_args()

    if args.func == 's3list':
        keys = list_keys_by_prefixes(
            bucket=args.bucket,
            prefixes=args.prefixes,
        )
        print(keys)
    if args.func == 's3download':
        output = download_by_keys(
            bucket=args.bucket,
            keys=args.keys,
            output_format=args.output_format,
        )
        print(output)
