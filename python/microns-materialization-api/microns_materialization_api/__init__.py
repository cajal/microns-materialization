from microns_utils import version_utils

__version__ = version_utils.get_package_version(
    package='microns-materialization-api', 
    check_if_latest=True, 
    check_if_latest_kwargs=dict(
        repo='microns-materialization', 
        user='cajal', 
        branch='main', 
        source='commit', 
        path_to_version_file='python/version.py', 
        tag=None
    )
)