const util = require('util');
const moment = require('moment');

module.exports = function (sails) {

    let config;
    let leader;
    let workerId;

    sails.on('ready', function() {
        workerId = _getRandomHash();
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
                    },
                    leader: {
                        redisField: 'leader',
                        ttl: 86400,
                        changeCooldown: 3600,
                        cronJob: {
                            name: 'schedulerBecomeLeader',
                            schedule: '*/5 * * * * *'
                        },
                    },
                    executor: {
                        ttl: 60,
                        cronJob: {
                            name: 'schedulerTaskExecutor',
                            schedule: '*/5 * * * * *'
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
        if (config.queue.enable && sails.hooks.queues.isReady(config.queue.name) && leader) {
            _pushTaskToQueue(task);
        } else {
            _executeTask(task);
        }
    }

    function _pushTaskToQueue(task) {
        if (sails.hooks.queues.isReady(config.queue.name)) {
            return;
        }
        let slug = task + '.' + moment().add(config.queue.executor.ttl, 's').valueOf();
        sails.hooks.queues.push(config.queue.name, slug)
          .then(() => {
              sails.log.info(`Task: ${task} pushed to queue.`);
          });
    }

    function _executeTaskFromQueue() {
        if (sails.hooks.queues.isReady(config.queue.name)) {
            return;
        }
        sails.hooks.queues.pop(config.queue.name)
            .then((res) => {
                if (res && res.message) {
                    let message = res.message.split('.');
                    let task = message[0];
                    let ttl = message[1];
                    if (moment().valueOf() > ttl) {
                        sails.log.debug('Task: `' + task + '` canceled.');
                        return _executeTaskFromQueue();
                    }
                    _executeTask(task)
                }
            })
            .catch(sails.log.error);
    }

    function _executeTask(task) {
        sails.config.scheduler.tasks[task].onTick();
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
            });
    }

    function _getCurrentLeader() {
        return new Promise((resolve) => {
            sails.getDatastore('cache').leaseConnection((db) => {
                (util.promisify(db.get).bind(db))(config.queue.redis.ns + ':' + config.queue.leader.redisField).then((found) => {
                    resolve(typeof found === 'string' ? JSON.parse(found) : null);
                });
            });
        });
    }

    function _setCurrentLeader() {
        let obj = JSON.stringify({
            workerId: workerId,
            timestamp: moment().valueOf(),
        });
        sails.getDatastore('cache').leaseConnection((db) => {
            (util.promisify(db.setex).bind(db))(config.queue.redis.ns + ':' + config.queue.leader.redisField, config.queue.leader.ttl, obj);
        });
    }

    function _getRandomHash() {
        return Math.random().toString(36).substring(7);
    }

}
