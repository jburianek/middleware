import os
import time

from middlewared.service import Service
from middlewared.plugins.config import FREENAS_DATABASE
from middlewared.plugins.datastore.connection import thread_pool
from middlewared.utils.threading import start_daemon_thread, set_thread_name

FREENAS_DATABASE_REPLICATED = f'{FREENAS_DATABASE}.replicated'
RAISE_ALERT_SYNC_RETRY_TIME = 1200  # 20mins (some platforms take 15-20mins to reboot)


class FailoverDatastoreService(Service):

    class Config:
        namespace = 'failover.datastore'
        private = True
        thread_pool = thread_pool

    async def sql(self, data, sql, params):
        if await self.middleware.call('system.version') != data['version']:
            return

        if await self.middleware.call('failover.status') != 'BACKUP':
            # We can't query failover.status on `MASTER` node (please see `hook_datastore_execute_write` for
            # explanations). Non-BACKUP nodes are responsible for checking their failover status.
            return

        await self.middleware.call('datastore.execute', sql, params)

    failure = False

    def is_failure(self):
        return self.failure

    def set_failure(self):
        self.failure = True
        try:
            self.send()
        except Exception as e:
            self.logger.warning('Error sending database to remote node on first replication failure: %r', e)

            def send_retry():
                set_thread_name('failover_datastore')

                raise_alert_time = RAISE_ALERT_SYNC_RETRY_TIME
                total_mins = raise_alert_time / 60
                sleep_time = 30
                while True:
                    raise_alert_time -= sleep_time
                    time.sleep(sleep_time)

                    if not self.failure:
                        # Someone sent the database for us
                        return

                    if (fs := self.middleware.call_sync('failover.status')) != 'MASTER':
                        self.logger.warning('Failover status changed to %s while retrying database send', fs)
                        self.failure = False
                        break

                    try:
                        self.middleware.call_sync('failover.datastore.send')
                    except Exception:
                        pass

                    if raise_alert_time <= 0 and self.failure:
                        self.middleware.call_sync('alert.oneshot_create', 'FailoverSyncFailed', {'mins': total_mins})
                        raise_alert_time = RAISE_ALERT_SYNC_RETRY_TIME

            start_daemon_thread(target=send_retry)

    def send(self):
        token = self.middleware.call_sync('failover.call_remote', 'auth.generate_token')
        self.middleware.call_sync('failover.send_file', token, FREENAS_DATABASE, FREENAS_DATABASE_REPLICATED)
        self.middleware.call_sync('failover.call_remote', 'failover.datastore.receive')

        self.failure = False
        self.middleware.call_sync('alert.oneshot_delete', 'FailoverSyncFailed', None)

    def receive(self):
        os.rename(FREENAS_DATABASE_REPLICATED, FREENAS_DATABASE)
        self.middleware.call_sync('datastore.setup')

    async def force_send(self):
        if await self.middleware.call('failover.status') == 'MASTER':
            await self.middleware.call('failover.datastore.set_failure')


def hook_datastore_execute_write(middleware, sql, params, options):
    # This code is executed in SQLite thread and blocks it (in order to avoid replication query race conditions)
    # No switching to the async context that will yield to database queries is allowed here as it will result in
    # a deadlock.

    if not options['ha_sync']:
        return

    if not middleware.call_sync('failover.licensed'):
        return

    if middleware.call_sync('failover.datastore.is_failure'):
        return

    try:
        middleware.call_sync(
            'failover.call_remote',
            'failover.datastore.sql',
            [
                {
                    'version': middleware.call_sync('system.version'),
                },
                sql,
                params,
            ],
            {
                'timeout': 10,
            },
        )
    except Exception as e:
        middleware.logger.warning('Error replicating SQL on the remote node: %r', e)
        middleware.call_sync('failover.datastore.set_failure')


async def setup(middleware):
    if not await middleware.call('system.is_enterprise'):
        return

    middleware.register_hook('datastore.post_execute_write', hook_datastore_execute_write, inline=True)
