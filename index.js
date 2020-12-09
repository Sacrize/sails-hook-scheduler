const util = require('util');
const moment = require('moment');

module.exports = function (sails) {

    let config;
    let leader;
    let workerId;

    sails.on('ready', function() {
        _checkDependencies();
        workerId = _getRandomHash();
        if (config.queue.enable) {
            _becomeLeader();
        }
    });

    return {
        defaults: {
            __configKey__: {
                becomeLeaderSchedule: '0 * * * * *',
                executeTaskSchedule: '* * * * * *',
                jobPrefix: 'scheduler_',
                queue: {
                    enable: false,
                    name: 'tasks',
                    redis: {
                        host: '127.0.0.1',
                        port: 6379,
                        options: {},
                        ns: 'scheduler',
                        password: undefined,
                        datastoreName: 'scheduler',
                    },
                    leader: {
                        redisField: 'leader',
                        ttl: 86400,
                        changeCooldown: 3600,
                        cronJob: {
                            name: 'schedulerBecomeLeader',
                            schedule: '0 * * * * *'
                        },
                    },
                    executor: {
                        cronJob: {
                            name: 'schedulerTaskExecutor',
                            schedule: '* * * * * *',
                            ttl: 60,
                        },
                    },
                },
                jobs: {

                },
            }
        },
        configure: function () {
            config = sails.config[this.configKey];
            sails.config.cron = sails.config.cron || {};
            if (config.queue.enable) {
                sails.config.queues = sails.config.queues || {};
                sails.config.queues[config.queue.name] = {};
                sails.config.cron[config.queue.leader.cronJob.name] = {
                    schedule: config.queue.leader.cronJob.schedule,
                    onTick: () => {
                        _becomeLeader();
                    },
                };
                sails.config.cron[config.queue.executor.cronJob.name] = {
                    schedule: config.queue.executor.cronJob.schedule,
                    onTick: () => {
                        _executeTaskFromQueue();
                    },
                };
                if (!(config.queue.redis.datastoreName in sails.config.datastores)) {
                    sails.config.datastores[config.queue.redis.datastoreName] = {
                        adapter: 'sails-redis',
                        ...config.queue.redis,
                    };
                }
            }
            for (let task of _.keys(config.jobs)) {
                sails.config.cron[config.jobPrefix + task] = {
                    schedule: config.jobs[task].schedule,
                    onTick() {
                        _pushToQueueOrExecute(task);
                    }
                }
            }
        }
    }

    function _pushToQueueOrExecute(task) {
        if (config.queue.enable) {
            if (leader) {
                _pushTaskToQueue(task);
            }
        } else {
            _executeTask(task);
        }
    }

    function _pushTaskToQueue(task) {
        if (!sails.hooks.queues.isReady(config.queue.name)) {
            return;
        }
        let slug = task + '.' + moment().add(config.queue.executor.cronJob.ttl, 's').valueOf();
        sails.hooks.queues.push(config.queue.name, slug)
          .then(() => {
              sails.log.info(`Task: ${task} pushed to queue`);
          });
    }

    function _executeTaskFromQueue() {
        if (!sails.hooks.queues.isReady(config.queue.name)) {
            return;
        }
        sails.hooks.queues.pop(config.queue.name)
            .then((res) => {
                if (res && res.message) {
                    let message = res.message.split('.');
                    let task = message[0];
                    let ttl = message[1];
                    if (moment().valueOf() > ttl) {
                        sails.log.debug('Task: `' + task + '` canceled');
                        return _executeTaskFromQueue();
                    }
                    _executeTask(task)
                }
            })
            .catch(sails.log.error);
    }

    function _executeTask(task) {
        sails.config.scheduler.jobs[task].onTick();
    }

    function _becomeLeader() {
        _getCurrentLeader()
            .then((obj) => {
                if (!obj || obj.timestamp < moment().subtract(config.queue.leader.changeCooldown, 's').valueOf()) {
                    _setCurrentLeader();
                } else {
                    leader = obj.workerId === workerId;
                    if (leader) {
                        sails.log.debug('I\'m cron leader!', workerId);
                    }
                }
            })
            .catch(sails.log.error);
    }

    function _getCurrentLeader() {
        return sails.getDatastore(config.queue.redis.datastoreName).leaseConnection(async (db) => {
            return await (util.promisify(db.get).bind(db))(config.queue.redis.ns + ':' + config.queue.leader.redisField);
        })
          .then((found) => {
            return typeof found === 'string' ? JSON.parse(found) : null;
        })
    }

    function _setCurrentLeader() {
        let obj = JSON.stringify({
            workerId: workerId,
            timestamp: moment().valueOf(),
        });
        sails.getDatastore(config.queue.redis.datastoreName).leaseConnection(async (db) => {
            return await (util.promisify(db.setex).bind(db))(config.queue.redis.ns + ':' + config.queue.leader.redisField, config.queue.leader.ttl, obj);
        })
          .then(() => {
              sails.log.debug('I\'m cron leader!', workerId);
          })
    }

    function _getRandomHash() {
        return Math.random().toString(36).substring(7);
    }

    /**
     * Required hooks to be installed in the project.
     * @private
     */
    function _checkDependencies() {
        let modules = [];
        if (config.queue.enable) {
            if (!sails.hooks.cron) {
                modules.push('sails-hook-cron');
            }
            if (!sails.hooks.queues) {
                modules.push('sails-hook-custom-queues');
            }
        }
        if (modules.length) {
            throw new Error('To use hook `sails-hook-scheduler`, you need to install the following modules: ' + modules.join(', '));
        }
    }
}
