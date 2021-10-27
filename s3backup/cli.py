import argparse
from s3backup.backups import BackupHandler
import logging


parser = argparse.ArgumentParser()
parser.add_argument('--config', '-c', type=str, required=True,
                    help="Specify the configuration filepath.")

commands = parser.add_subparsers(help="Commands", dest="command")

backup_parser = commands.add_parser('backup')
backup_parser.add_argument('--rename', '-rn', dest='rename_to')
backup_parser.add_argument('--target', '-t', dest='backup_target')
restore_parser = commands.add_parser('restore')
list_parser = commands.add_parser('list')

logger = logging.getLogger('s3_backup:cli')
logging.basicConfig(
    format="%(asctime)-15s %(message)s"
)


def main():
    args = parser.parse_args()

    handler = BackupHandler.from_file(args.config)
    try:
        if args.command == "restore":
            handler.restore()
        elif args.command == "backup":
            handler.backup(
                rename_to=args.rename_to,
                backup_target=args.backup_target
            )
        elif args.command == "list":
            collection = handler.store.list_objects()
            print("Bucket List (lol):")
            for name in collection.filenames:
                print("  - {}".format(name))
        else:
            raise Exception("Invalid command.")
    except Exception as e:
        logger.error(e)
