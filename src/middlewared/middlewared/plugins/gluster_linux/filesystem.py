import errno
import os

from base64 import b64encode, b64decode
from middlewared.schema import accepts, Bool, Dict, Int, List, Str, Ref
from middlewared.service import Service, CallError, job, private
from middlewared.schema import Path
from middlewared.validators import UUID
from middlewared.plugins.gluster_linux.pyglfs_utils import glfs


class GlusterFilesystemService(Service):

    """
    Current todo list:
    * add xattr support
    * lookup of volfile servers from gluster volume info
    * add wrapper for handle.fts_open() method to iterate fs path
    """
    class Config:
        namespace = 'gluster.filesystem'
        cli_namespace = 'service.gluster.filesystem'
        private = True

    @private
    def show_volume_handles(self):
        return {h['name']: h['options'] for h in glfs.handles.values()}

    @private
    def close_volume_handles(self):
        for entry in list(glfs.handles.values()):
            with entry['lock']:
                # Volume closes automatically in dealloc function
                entry['handle_internal'] = None

    @private
    def glfs_object_handle_to_dict(self, obj):
        """
        Convert the glfs object into something JSON serializable.
        stat info is included because it's already there.

        NOTE: obj.name will not be available for handles opened by
        UUID, and so we don't include for consistency sake.
        """
        return {
            'uuid': obj.uuid,
            'file_type': obj.file_type,
            'stat': {
                'st_mode': obj.cached_stat.st_mode,
                'st_ino': obj.cached_stat.st_ino,
                'st_dev': obj.cached_stat.st_dev,
                'st_nlink': obj.cached_stat.st_nlink,
                'st_size': obj.cached_stat.st_size,
                'st_uid': obj.cached_stat.st_uid,
                'st_gid': obj.cached_stat.st_gid,
            }
        }

    @private
    def get_object_handle(self, vol, uuid):
        """
        convert the provided UUID to a handle.
        It's rather hard to know what the UUID of
        the root of gluster volume is off the top
        of one's head and so for convenience feature,
        we treat None type as 'get the root of volume'.
        """
        if uuid is None:
            hdl = vol.get_root_handle()
        else:
            hdl = vol.open_by_uuid(uuid)

        return hdl

    @accepts(Dict(
        'glfs-lookup',
        Str('volume_name', required=True),
        Str('parent_uuid', null=True, default=None, validators=[UUID()]),
        Str('path', required=True),
        Dict(
            'options',
            Bool('symlink_follow', default=False)
        ),
        Dict(
            'gluster-volume-options',
            List(
                'volfile_servers',
                null=True, default=None,
                items=[Dict(
                    Str('host', required=True),
                    Str('proto', enum=['TCP', 'RDMA'], default='TCP'),
                    Int('port', default=0),
                )],
            ),
            List(
                'translators',
                items=[Dict(
                    Str('xlator_name', required=True),
                    Str('key', required=True),
                    Str('value', required=True)
                )],
            ),
            Path('log_file'),
            Int('log_level'),
            register=True
        )
    ))
    def lookup(self, data):
        """
        Get a handle for an existing glusterfs filesystem object. API
        doesn't differentiate between looking up a file vs a directory.

        Parameters:
        ----------
        `volume_name` - the name of the glusterfs volume where the object is located.
        `parent_uuid` - the UUID of the parent object. `None` here means volume root.
        `path` - path relative to the parent object specified by `parent_uuid`
        `options` - options to be passed to the lookup. Currently the only exposed
        option is `symlink_follow` which follows symlinks to their target.

        Gluster volume configuration parameters:
        `gluster-volume-options` - these options are related to the glusterfs virtual
        mount.
        `volfile_servers` - a list of glusterfs volfile servers available to use
        for the virtual mount. Multiple volfile servers provides redundancy.
        `log_file` - optional local NAS path to write log file for the volume.
        `log_level` - specifies verbosity of logging for the virtual mount.

        Returns:
        -------
        Dict containing information about the results of the lookup. The `UUID`
        key in the returned dictionary can be used for further filesystem operations.
        """
        with glfs.get_volume_handle(data['volume_name'], data['gluster-volume-options']) as vol:
            parent = self.get_object_handle(vol, data['parent_uuid'])
            obj = parent.lookup(data['path'], **data['options'])
            return self.glfs_object_handle_to_dict(obj)

    @accepts(Dict(
        'glfs-create-file',
        Str('volume_name', required=True),
        Str('parent_uuid', null=True, default=None, validators=[UUID()]),
        Str('path', required=True),
        Dict(
            'options',
            Int('mode', default=0o644),
        ),
        Ref('gluster-volume-options')
    ))
    def create_file(self, data):
        """
        Create a file on the specified glusterfs filesystem at the specified
        path relative to the specified parent uuid.

        Parameters:
        ----------
        `volume_name` - the name of the glusterfs volume where the object is located.
        `parent_uuid` - the UUID of the parent object. `None` here means volume root.
        `path` - path relative to the parent object specified by `parent_uuid`
        `options` - options to be passed to the create operation.
        `mode` - permissions to set on the newly-created file.

        Returns:
        -------
        Dict containing information about newly created file. The `UUID`
        key in the returned dictionary can be used for further filesystem operations.
        """
        additional_kwargs = {"flags": os.O_CREAT | os.O_RDWR}
        with glfs.get_volume_handle(data['volume_name'], data['gluster-volume-options']) as vol:
            parent = self.get_object_handle(vol, data['parent_uuid'])
            obj = parent.create(data['path'], **(data['options'] | additional_kwargs))
            return self.glfs_object_handle_to_dict(obj)

    @accepts(Dict(
        'glfs-mkdir',
        Str('volume_name', required=True),
        Str('parent_uuid', null=True, default=None, validators=[UUID()]),
        Str('path', required=True),
        Dict(
            'options',
            Int('mode', default=0o755)
        ),
        Ref('gluster-volume-options')
    ))
    def mkdir(self, data):
        """
        Create a directory on the specified glusterfs filesystem at the specified
        path relative to the specified parent uuid.

        Parameters:
        ----------
        `volume_name` - the name of the glusterfs volume where the object is located.
        `parent_uuid` - the UUID of the parent object. `None` here means volume root.
        `path` - path relative to the parent object specified by `parent_uuid`
        `options` - options to be passed to the create operation.
        `mode` - permissions to set on the newly-created directory.

        Returns:
        -------
        Dict containing information about newly created directory. The `UUID`
        key in the returned dictionary can be used for further filesystem operations.

        """
        with glfs.get_volume_handle(data['volume_name'], data['gluster-volume-options']) as vol:
            parent = self.get_object_handle(vol, data['parent_uuid'])
            obj = parent.mkdir(data['path'], **data['options'])
            return self.glfs_object_handle_to_dict(obj)

    @accepts(Dict(
        'glfs-unlink',
        Str('volume_name', required=True),
        Str('parent_uuid', null=True, default=None, validators=[UUID()]),
        Str('path', required=True),
        Ref('gluster-volume-options')
    ))
    def unlink(self, data):
        """
        Remove the glusterfs filesystem object at the specified
        path relative to the specified parent uuid.

        Parameters:
        ----------
        `volume_name` - the name of the glusterfs volume where the object is located.
        `parent_uuid` - the UUID of the parent object. `None` here means volume root.
        `path` - path relative to the parent object specified by `parent_uuid`

        Returns:
        -------
        None
        """
        with glfs.get_volume_handle(data['volume_name'], data['gluster-volume-options']) as vol:
            parent = self.get_object_handle(vol, data['parent_uuid'])
            parent.unlink(data['path'])

    @accepts(Dict(
        'glfs-contents',
        Str('volume_name', required=True),
        Str('uuid', required=True, validators=[UUID()]),
        Dict(
            'options',
            Str('file_output_type', enum=['STRING', 'BINARY'], default='STRING')
        ),
        Ref('gluster-volume-options')
    ))
    def contents(self, data):
        """
        Get the contents of a glusterfs filesystem object.

        Parameters:
        ----------
        `volume_name` - the name of the glusterfs volume where the object is located.
        `uuid` - the UUID of the object.
        `file_output_type` - for objects that are files containing data that is not
        JSON-serializable `BINARY` may be specified in order to return the data
        as a base64-encoded string.

        Returns:
        -------
        Return type depends on file type of object:
        - Directory - List containing directory contents
        - File - contents of file as either string or base64 encoded string
        - Symlink - readlink return for symlink
        """
        with glfs.get_volume_handle(data['volume_name'], data['gluster-volume-options']) as vol:
            target = self.get_object_handle(vol, data['uuid'])
            contents = target.contents()
            if target.file_type['parsed'] == 'FILE':
                if data['options']['file_output_type'] == 'STRING':
                    output = contents.decode()

                elif data['options']['file_output_type'] == 'BINARY':
                    output = b64encode(contents).decode()

            else:
                output = contents

            return output

    @accepts(Dict(
        'glfs-setattrs',
        Str('volume_name', required=True),
        Str('uuid', required=True, validators=[UUID()]),
        Dict(
            'options',
            Int('uid', default=-1),
            Int('gid', default=-1),
            Int('mode'),
            Bool('recursive', default=False)
        ),
        Ref('gluster-volume-options')
    ))
    @job()
    def setattrs(self, job, data):
        """
        Middleware `job` to set specified attributes on the specified glusterfs object.

        Parameters:
        ----------
        `volume_name` - the name of the glusterfs volume where the object is located.
        `uuid` - the UUID of the object.
        `uid` - the owner uid to set on the object. Default is to leave as-is.
        `gid` - the owner gid to set on the object. Default is to leave as-is.
        `mode` - the file mode (permissions) to set on the object. Default is to leave as-is.
        `recursive` - if object is directory perform operation recursively. NOTE: this
        may take a sigificant amount of time to return.

        Returns:
        ------
        The job will return glusterfs object information with updated stat output reflecting
        changes.
        """

        if data['options']['recursive']:
            # Trade-off for recursive jobs. These may be _very_ long-running and so
            # execute under dedicated virtual mount. This will add a few seconds for
            # temporary mount teardown, but should be acceptable for a long-running job.
            tmp_vol = glfs.init_volume_mount(data['volume_name'], data['gluster-volume-options'])
            target = self.get_object_handle(tmp_vol, data['uuid'])
            target.stat()
            if target.file_type['parsed'] != 'DIRECTORY':
                raise CallError('Gluster filesystem object is not a directory.', errno.ENOTDIR)

            target.setattrs(**data['options'])
            target.stat()
            return self.glfs_object_handle_to_dict(target)

        with glfs.get_volume_handle(data['volume_name'], data['gluster-volume-options']) as vol:
            target = self.get_object_handle(vol, data['uuid'])
            target.setattrs(**data['options'])
            target.stat()
            return self.glfs_object_handle_to_dict(target)

    @accepts(Dict(
        'glfs-pread',
        Str('volume_name', required=True),
        Str('uuid', required=True, validators=[UUID()]),
        Dict(
            'options',
            Int('offset', required=True),
            Int('cnt', required=True)
        ),
        Ref('gluster-volume-options')
    ))
    def pread(self, data):
        """
        Read a specified number of bytes from a glusterfs object at the specified offset.

        Parameters:
        ----------
        `volume_name` - the name of the glusterfs volume where the object is located.
        `uuid` - the UUID of the object.
        `offset` - the offset from beginning of file from which to read.
        `cnt` - number of bytes to read.

        Returns:
        ------
        base64-encoded string containing specified bytes (length from offset) of file.
        """
        with glfs.get_volume_handle(data['volume_name'], data['gluster-volume-options']) as vol:
            fd = self.get_object_handle(vol, data['uuid']).open(os.O_RDONLY)
            bytes = fd.pread(data['options']['offset'], data['options']['cnt'])
            return b64encode(bytes).decode()

    @accepts(Dict(
        'glfs-pwrite',
        Str('volume_name', required=True),
        Str('uuid', required=True, validators=[UUID()]),
        Str('payload', required=True),
        Str('payload_type', enum=['STRING', 'BINARY'], default='STRING'),
        Dict(
            'options',
            Int('offset', default=0),
        ),
        Ref('gluster-volume-options')
    ))
    def pwrite(self, data):
        """
        Write a specified number of bytes from a glusterfs object at the specified offset.

        Parameters:
        ----------
        `volume_name` - the name of the glusterfs volume where the object is located.
        `uuid` - the UUID of the object.
        `payload` - the payload to write to the specified glusterfs object.
        `payload_type` - specify whether this is unicode string or base64-encoded binary data.
        `offset` - offset from beginning of file from which to write `payload`

        Returns:
        ------
        None
        """
        with glfs.get_volume_handle(data['volume_name'], data['gluster-volume-options']) as vol:
            if data['payload_type'] == 'STRING':
                payload = data['payload'].encode()
            elif data['payload_type'] == 'BINARY':
                payload = b64decode(data['payload'])

            fd = self.get_object_handle(vol, data['uuid']).open(os.O_RDWR)
            fd.pwrite(payload, data['options']['offset'])

    @accepts(Dict(
        'glfs-rmtree',
        Str('volume_name', required=True),
        Str('parent_uuid', null=True, required=True, validators=[UUID()]),
        Str('path', required=True),
        Ref('gluster-volume-options')
    ))
    @job()
    def rmtree(self, job, data):
        """
        Recursively unlink the contents of the gluster object specified by UUID.

        Parameters:
        ----------
        `volume_name` - the name of the glusterfs volume where the object is located.
        `parent_uuid` - the UUID of the parent object.
        `path` - path to recursively remove

        Returns:
        ------
        None
        """
        def prune_tree(root, target_depth, dir_list):
            # This function resets us back to the depth specified
            if not dir_list:
                return False

            while dir_list[-1].depth != target_depth:
                to_delete = dir_list.pop()
                if dir_list:
                    dir_list[-1].handle.unlink(to_delete.name)
                else:
                    root.unlink(to_delete.name)
                    return False

            return True

        vol = glfs.init_volume_mount(data['volume_name'], data['gluster-volume-options'])
        parent = self.get_object_handle(vol, data['parent_uuid'])
        hdl = parent.lookup(data['path'])

        # pyglf.FTSEntry objects track their depth in directory tree
        # allowing us to basically walk back directory components that are
        # empty. In most cases this list will have fewer than 10 items (unless
        # the tree is exceptionally deep).
        dir_list = []

        for entry in hdl.fts_open():
            last_dir = dir_list[-1] if dir_list else None

            if last_dir and entry.depth <= last_dir.depth:
                if not prune_tree(hdl, entry.depth, dir_list):
                    hdl.unlink(entry.name)
                    continue

                to_delete = dir_list.pop()

                if not dir_list:
                    hdl.unlink(to_delete.name)
                    hdl.unlink(entry.name)
                    continue

                dir_list[-1].handle.unlink(to_delete.name)
                dir_list[-1].handle.unlink(entry.name)
                continue

            if entry.file_type == 'DIRECTORY':
                dir_list.append(entry)
                continue

            if last_dir:
                last_dir.handle.unlink(entry.name)
            else:
                hdl.unlink(entry.name)

        # Remove any remaining empty tree components before deleting our target path
        prune_tree(hdl, 0, dir_list)
        if dir_list:
            hdl.unlink(dir_list[-1].name)

        parent.unlink(data['path'])

    @accepts(Dict(
        'glfs-setperm',
        Str('volume_name', required=True),
        Str('uuid', required=True, validators=[UUID()]),
        Dict(
            'options',
            Int('uid', default=-1),
            Int('gid', default=-1),
            Int('mode'),
            Str('acl_operation', enum=['STRIP', 'INHERIT']),
        ),
        Ref('gluster-volume-options')
    ))
    @job()
    def setperm(self, job, data):
        job.set_progress(50, 'Initializing glusterfs volume mount.')
        vol = glfs.init_volume_mount(data['volume_name'], data['gluster-volume-options'])
        hdl = self.get_object_handle(vol, data['uuid'])

        acl_op = data['options'].pop('acl_operation', None)
        if acl_op == 'INHERIT':
            fd = hdl.open(os.O_RDONLY)
            xat_buf = fd.fgetxattr('system.posix_acl_default')

        if data['options']['uid'] == -1 and dat['options']['gid'] == -1 and data['options']['mode'] is None:
            attrs = None
        else:
            attrs = data['options']

        job.set_progress(50, 'Preparing to set permissions.')

        for idx, entry in enumerate(hdl.fts_open()):
            # Update status only every 100 files
            if idx % 100 == 0:
                job.set_progress(50, f'Setting permissions: {entry.parent_path}/{entry.name}')

            if acl_op == 'INHERIT':
                fd = entry.handle.open(os.O_RDWR)
                if entry.file_type == 'DIRECTORY':
                    fd.fsetxattr('system.posix_acl_default', xat_buf)

                fd.fsetxattr('system.posix_acl_access', xat_buf)

            elif acl_op == 'STRIP':
                # TODO: we should probably add option to pyglfs to ignore errors on rmxattr
                # so that we can avoid listing them per file.
                fd = entry.handle.open(os.O_RDWR)
                xattrs = fd.flistxattr()
                if 'system.posix_acl_default' in xattrs:
                    fd.fremovexattr('system.posix_acl_default')

                if 'system.posix_acl_access' in xattrs:
                    fd.fremovexattr('system.posix_acl_access')

            if attrs:
                entry.handle.setattrs(**attrs)
