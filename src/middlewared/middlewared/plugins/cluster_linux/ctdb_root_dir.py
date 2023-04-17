import errno
import os

import pyglfs
from middlewared.service import Service, CallError, job
from middlewared.plugins.gluster_linux.utils import GTDBConfig

LOCK = 'ctdb-init-lock'
ROOT_DIR_NAME = 'ctdb-root-dir'
CTDB_ROOT_DIR_VOLUME_LOCATION = GTDBConfig.CTDB_ROOT_DIR_VOLUME_LOCATION.value


def rmtree(handle):
    for i in handle.lookup(ROOT_DIR_NAME).fts_open():
        if i.file_type == 'DIRECTORY':
            rmtree


class CtdbRootDirService(Service):

    class Config:
        namespace = 'ctdb.root_dir'
        private = True

    def get_root_handle(self, gvol_name):
        return pyglfs.Volume({
            'volume_name': gvol_name,
            'volfile_servers': [{'host': '127.0.0.1', 'proto': 'tcp', 'port': 0}]
        }).get_root_handle()

    @job(lock=LOCK)
    def init(self, job, gvol_name):
        """
        This method will initialize the ctdb directory responsible for storing
        files used by ctdb daemon for cluster operations. Without this directory,
        ctdb daemon and therefore SMB active-active shares will not work.

        We use the native gluster file I/O API via the pyglfs module. This means
        we don't need to go through a local fuse mount.

        Also, since this directory is stored at the root of the gluster volume, it's
        imperative that we protect this directory via permissions. We lock this down
        to only root user and nobody else may access it.
        """
        root_handle = self.get_root_handle(gvol_name)

        # create the root dir
        try:
            root_handle.mkdir(ROOT_DIR_NAME)
        except pyglfs.GLFSError as e:
            if e.errno != errno.EEXIST:
                raise CallError(f'Failed to create {ROOT_DIR_NAME!r}: {e}')

        # set perms
        dir_fd = root_handle.lookup(ROOT_DIR_NAME).open(os.O_DIRECTORY)
        stat = dir_fd.fstat()
        if stat.st_mode & 0o700 != 0:
            try:
                dir_fd.fchmod(0o700)
            except Exception:
                # this isn't fatal but still need to log something
                self.logger.warning('Failed to change permissions on %r', ROOT_DIR_NAME, exc_info=True)

        # change user/group owner to root/root (-1 means leave unchanged)
        uid = 0 if stat.st_uid != 0 else -1
        gid = 0 if stat.st_gid != 0 else -1
        if uid == 0 or gid == 0:
            dir_fd.fchown(uid, gid)

        with open(CTDB_ROOT_DIR_VOLUME_LOCATION, 'w') as f:
            f.write(gvol_name)

    @job(lock=LOCK)
    def teardown(self):
        gvol = self.get_gluster_volume_location()
        if gvol is None:
            return

        rmtree(self.get_root_handle(gvol))

        for i in self.get_root_handle(gvol).lookup(ROOT_DIR_NAME).fts_open():
            if i.file_type == 'DIRECTORY':

            if i.file_type == 'FILE':
                pass

    def rem_gluster_volume_location(self):
        init_job = self.middleware.call('ctdb.root_dir.teardown')
        init_job.wait_sync(raise_error=True)

    def set_gluster_volume_location(self, gvol_name):
        init_job = self.middleware.call('ctdb.root_dir.init', gvol_name)
        init_job.wait_sync(raise_error=True)

    def get_gluster_volume_location(self):
        for vol in self.middleware.call_sync('gluster.volume.list'):
            try:
                self.middleware.call_sync('gluster.filesystem.lookup', {'volume_name': vol, 'path': ROOT_DIR_NAME})
            except Exception as e:
                if e.errno == errno.ENOENT:
                    continue
                else:
                    raise CallError('Unhandled exception: {e!r}')
            else:
                return vol
