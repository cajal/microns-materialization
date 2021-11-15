from microns_utils import config_utils

_repo = 'microns-materialization'
_package = _repo

__version__ = config_utils.get_package_version(repo=_repo, package=_package)