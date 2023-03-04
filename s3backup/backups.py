import os
import json
import glob
import time
import yaml
import boto3
import tarfile
import tempfile
import logging
import random
import shutil
from distutils import dir_util as du
from botocore import exceptions as bexc


logger = logging.getLogger(__name__)


def maybe_format(msg, obj, kwargs, variable):
    # type: (str, Any, dict[str, Any], Optional[Tuple[str, str]]) -> str
    """Attempts to format a message with variable information.

    Intended to wrap a bound method, this takes the binding object and the kwargs
    of its wrapped method, and (optionally) a two-tuple of variable names. The first
    element of the tuple is the name of the wrapped method's kwarg to use in formatting;
    the second element is the name of the object attribute to use if the kwarg isn't set."""
    if variable is not None:
        fn_param_name, self_param_name = variable
        variable = kwargs.get(fn_param_name, getattr(obj, self_param_name))
        return msg.format(variable)
    return msg


def processlog(startmsg, successmsg, failmsg, variable=None):
    # type: (str, str, str, Optional[Tuple[str, str]]) -> callable
    # noinspection PyInterpreter
    """Wrapper around a class method to log its lifecycle.

        Takes a message to print on start, success and failure, and a two-tuple of variable
          names: one which is in the wrapped function's kwargs, and another that may be an
          attribute or property of `self` on the object being wrapped (with the first being
          used if available, otherwise the second). These variables can be inserted into any
          of the messages with a pair of curly braces (e.g. "Reason: {}").

        """
    def wrapper(f):
        def inner(self, *args, **kwargs):
            msg = maybe_format(startmsg, self, kwargs, variable)
            logger.info(msg)
            try:
                rv = f(self, *args, **kwargs)
            except Exception as e:
                msg = maybe_format(failmsg, self, kwargs, variable)
                logger.error("{}: {}: {}".format(msg, e.__class__.__name__, e))
                raise
            msg = maybe_format(successmsg, self, kwargs, variable)
            logger.info(msg)
            return rv
        return inner
    return wrapper


def load_config(path):
    """Load either a JSON or YAML file into a dict."""
    try:
        with open(path) as f:
            contents = f.read()
    except Exception as e:
        print("Unable to load config at:\n  {}".format(path))
        print("Reason: {}".format(e))
        raise

    extension = path.split('.')[-1].lower()

    if extension in ('yaml', 'yml'):
        return yaml.safe_load(contents)

    if extension == 'json':
        return json.loads(contents)

    raise Exception("Unable to load config")


class StagingContext(object):
    """Allows us to stage a file or files within a temp directory."""

    def __init__(self, tmpdir='/tmp/'):
        self.tmpdir = self._initialize(tmpdir)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        shutil.rmtree(self.tmpdir)

    def _initialize(self, tmpdir):
        dir = str(random.randint(100000000, 999999999))
        path = os.path.join(tmpdir, dir)
        os.makedirs(path)
        return path

    def stage(self, target, includes=None):
        self.isdir = os.path.isdir(target)
        if not self.isdir and includes:
            raise Exception("Includes can only be used with with a directory.")

        target_name = os.path.normpath('{}/'.format(target)).split(os.sep)[-1]
        self.basepath = os.path.join(self.tmpdir, target_name)
        self.targetpath = self.basepath
        self.target_name = target_name

        if self.isdir:
            includes = includes or ['*']

            objects = []
            for i in includes:
                objects.extend(glob.glob(os.path.join(target, i)))

            for obj in objects:
                end_path = obj.split(target)[-1].lstrip(os.sep)
                path = os.path.join(self.targetpath, end_path)
                if os.path.isdir(obj):
                    du.copy_tree(obj, path, preserve_mode=True,
                                 preserve_symlinks=True)
                else:
                    try:
                        os.makedirs(os.path.dirname(path))
                    except OSError:
                        pass
                    shutil.copy2(obj, path)

        else:
            shutil.copy2(target, self.basepath)

        return self.basepath

    def compress(self, rename_to=None):
        if rename_to:
            tar_name = rename_to
        else:
            tar_name = '{}.tgz'.format(self.target_name)
        tar_path = os.path.join(self.tmpdir, tar_name)

        with tarfile.open(tar_path, 'w:gz') as tar:
            tar.add(self.targetpath, arcname=self.target_name)

        self.targetpath = os.path.join(self.tmpdir, tar_name)

        return self.targetpath

    def path(self):
        return self.targetpath

    def name(self):
        return os.path.normpath(self.targetpath + os.sep).split(os.sep)[-1]


class BackupHandler(object):
    """Interface for back-up/restoration from s3."""

    def __init__(self, store, backupdir, logconf=None, includes=None):
        self.store = store
        self.backupdir = backupdir
        self.includes = includes
        self._setup_logging(logconf)

    @classmethod
    def from_file(cls, filepath):
        """Load a config file and use it to instantiate."""
        config = load_config(filepath)

        backupconf = config["backup"]
        store = BackupStore(
            bucket=config["bucket"],
            path=config.get('path'),
            profile=config.get('profile'),
            retain=backupconf.get('retain'),
        )
        handler = cls(
            store=store,
            backupdir=backupconf.get('backup_target'),
            includes=backupconf.get('includes'),
            logconf=config.get('logging'),
        )
        return handler

    @processlog(
        startmsg="Attempting to backup directory {}.",
        successmsg="Backup of directory {} was successful!",
        failmsg="Backup failed for directory {}.",
        variable=('backup_target', 'backupdir')
    )
    def backup(self, backup_target=None, rename_to=None):
        """Tar the folder (includes only) and upload it to s3."""
        backup_target = self._get_dirname(backup_target or self.backupdir)
        with StagingContext() as stage:
            stage.stage(backup_target, includes=self.includes)
            stage.compress(rename_to)
            final_path = stage.path()
            self.store.upload(final_path, stage.name())

    def prune(self, retain=5):
        """Tar the folder (includes only) and upload it to s3."""
        with StagingContext() as stage:
            self.store._prune_bucket(retain=retain)

    @processlog(
        startmsg="Attempting to restore to {}.",
        failmsg="Restore failed for {}.",
        successmsg="Restore of {} successful!",
        variable=('directory', 'backupdir')
    )
    def restore(self, directory=None):
        """Restore a directory from the latest backup in the S3 Bucket.

        Params:
            - directory: <str> Path to restore file to.
                If unspecified, we use the `self.backupdir`
        """
        logger.info('Attempting to restore.')
        directory = self._get_dirname(directory or self.backupdir)

        with tempfile.NamedTemporaryFile() as f:
            dirname = os.path.dirname(f.name)
            filename = f.name.split(os.sep)[-1]
            self.store.download(self.store.LATEST,
                                dirname,
                                as_filename=filename)

            self.extract(f.name, directory)
        logger.info('Successfully "{}" restored from backup.'
                    .format(directory))

    def log(self, msg, level=logging.INFO):
        """Simplifier for logging."""
        logger.log(level, msg)

    def extract(self, tarpath, directory):
        """Excract the file from the tarfile path to the directory."""
        if not os.path.isfile(directory):
            directory = os.path.dirname(directory)

        with tarfile.open(tarpath) as f:
            f.extractall(directory)

    def _get_dirname(self, path):
        expanded = os.path.expanduser("{}/".format(path))
        return os.path.dirname(expanded)

    def _get_tarname(self, path):
        timestamp = int(time.time())
        dir_only = os.path.normpath(path).split(os.sep)[-1]
        return "{}_{}.tar.gz".format(dir_only, timestamp)

    def _setup_logging(self, logconf):
        if not logconf:
            return

        try:
            logpath = logconf['filepath']
            logpath = os.path.expanduser(logpath)
            loglevel = logconf.get('loglevel', 'info')

            logging.basicConfig(filename=logpath, format=logconf['format'])
            logger.setLevel(loglevel.upper())
        except KeyError:
            raise Exception(
                "Unable to set up logging. Ensure that you have "
                "'filepath' set in your logging config."
            )


class Collection(object):
    """List-like represenation of a colleciton of objects from s3."""

    def __init__(self, objects):
        self.objects = objects
        self._initialized = True

    @property
    def filenames(self):
        """Return the filenames of this Collection's objects."""
        return [o.filename for o in self.objects]

    def get(self, value, key='filename'):
        """Get an object based on a key-value query."""
        for obj in self.objects:
            if getattr(obj, key) == value:
                return obj
        return None

    def filterd(self, filter_fn):
        """Filter this Collection's objects and return a new one."""
        filtered = filter(filter_fn, self.objects)
        return self.__class__(objects=filtered)

    def ordered(self, order_by='modified', desc=True):
        """Order this collection's objects and return a new one."""
        ordered = sorted(self.objects,
                         key=lambda x: getattr(x, order_by),
                         reverse=desc)
        return self.__class__(objects=ordered)

    def __getitem__(self, g):
        """Return a Collection when slicing."""
        res = self.objects.__getitem__(g)
        if not isinstance(res, (list, tuple)):
            return res
        return self.__class__(objects=res)

    def __repr__(self):
        return str(list(self))

    def __iter__(self):
        return iter(self.objects)

    def __len__(self):
        return len(self.objects)


class BucketObject(object):
    """Class represenation of the metadata of an s3 Bucket object."""

    def __init__(self, contents):
        """Map the s3 key-value to a more readable/pythonic one.

        Note the _initialized... This makes this object immutable.
        """
        self.modified = contents["LastModified"]
        self.filename = contents["Key"]
        self.etag = contents["ETag"]
        self.size = contents["Size"]
        self._initialized = True

    def as_dict(self):
        """Return the BucketObject as a dict."""
        adict = {
            'modified': self.modified,
            'filename': self.filename,
            'size': self.size,
            'etag': self.etag
        }
        return adict

    def __getattr__(self, key):
        """Allow for dict-like attribute fetching."""
        return getattr(self.__dict__, key)

    def __setattr__(self, key, value):
        """Disallow setting of attributes after creation."""
        if hasattr(self, '_initialized'):
            raise Exception("Bucket objects are immutable.")
        super(BucketObject, self).__setattr__(key, value)


class BackupStore(object):
    """Interface for downloading & uploading from a single s3 bucket."""

    LATEST = 'backups.latest'

    def __init__(self, bucket, path=None, profile=None, retain=None):
        self._bucket = bucket
        self._path = path or ''
        self._profile = profile
        self._retain = retain

        self.session = boto3.Session(profile_name=profile)
        self.resource = self.session.resource('s3')
        self.client = self.session.client('s3')

    def download(self, target, localpath, as_filename=None):
        """Download a single file from the S3 bucket.

        Params:
            - target: <str> name of file in the bucket.
            - localpath: <str> path (excluding filename) to download to
            - as_filename (optional): <str> what we will change the filename to

        Returns: filepath the file was downloaded to
        """
        if target == self.LATEST:
            try:
                filename = self.list_objects().filenames[-1]
            except IndexError:
                raise EnvironmentError("Error: Bucket appears to be empty")
        elif as_filename:
            filename = as_filename
        else:
            raise ValueError('Target is not latest and filepath not given; cannot continue')

        try:
            local = os.path.join(localpath, filename)
            self.client.download_file(self._bucket, filename, local)
        except bexc.ClientError as e:
            print("Download Exception: {}".format(e))
            raise

        return local

    def upload(self, localpath, target=None):
        """Upload a file or folder to the repository.

        Params:
          - `localpath` filepath to be uploaded
          - `target` path on the bucket (will change filename)

        """
        path = os.path.normpath(localpath)
        if not target:
            target = os.path.split(path)[1]

        self.client.upload_file(path, self._bucket, os.path.join(self._path, target))

        if self._retain:
            self._prune_bucket(self._retain)

    def list_objects(self):
        """List all objects found in the s3 bucket."""
        raw = self.client.list_objects(Bucket=self._bucket, Prefix=self._path)
        objects = [BucketObject(o) for o in raw["Contents"]]
        return Collection(objects=objects)

    def delete(self, collection):
        """Delete a collection of objects from the s3 bucket."""
        if not isinstance(collection, Collection):
            if isinstance(collection, (tuple, list)):
                collection = Collection(collection)
            else:
                collection = Collection([collection])

        try:
            objects = [{'Key': n} for n in collection.filenames]
        except AttributeError:
            raise AttributeError(
                "Delete method takes a Collection of BucketObjects")
        self.client.delete_objects(
            Delete={"Objects": objects},
            Bucket=self._bucket,
        )

    def _prune_bucket(self, retain, patterns=['deb', 'tar.gz']):
        objects = self.list_objects().ordered(order_by='modified')
        if objects:
            for pattern in patterns:
                objects_by_pattern = [obj for obj in objects if pattern in obj.filename]
                if len(objects_by_pattern) > retain:
                    to_prune = objects_by_pattern[retain:]
                    self.delete(to_prune)
