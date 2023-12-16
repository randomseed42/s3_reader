import argparse

from .s3_reader import download_by_keys, download_by_prefixes, list_keys_by_prefixes


def main():
    parser = argparse.ArgumentParser('S3 Reader')
    subparser = parser.add_subparsers(dest='func')

    parser_list = subparser.add_parser('s3list', help='Get AWS S3 files list')
    parser_list.add_argument('-b', '--bucket', required=True, help='AWS bucket name')
    parser_list.add_argument('-p', '--prefixes', nargs='+', required=True, help='AWS prefix name')

    parser_download = subparser.add_parser('s3download', help='Get AWS S3 files data')
    parser_download.add_argument('-b', '--bucket', required=True, help='AWS bucket name')
    parser_download_group = parser_download.add_mutually_exclusive_group(required=True)
    parser_download_group.add_argument('-k', '--keys', nargs='+', help='AWS file key name')
    parser_download_group.add_argument('-p', '--prefixes', nargs='+', help='AWS file prefix name')
    parser_download.add_argument('-d', '--output_dir', default='', help='Download target directory')

    args = parser.parse_args()

    if args.func == 's3list':
        keys = list_keys_by_prefixes(
            bucket=args.bucket,
            prefixes=args.prefixes,
        )
        print(keys)
    elif args.func == 's3download':
        if args.keys:
            download_by_keys(
                bucket=args.bucket,
                keys=args.keys,
                output_format='bytes',
                output_dir=args.output_dir,
            )
        elif args.prefixes:
            download_by_prefixes(
                bucket=args.bucket,
                prefixes=args.prefixes,
                output_format='bytes',
                output_dir=args.output_dir,
            )
        else:
            raise ValueError(f'--keys or --prefixes argument is required.')
    else:
        raise ValueError(f'Invalid command {args.func}')
