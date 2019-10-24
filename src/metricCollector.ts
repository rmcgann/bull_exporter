import bull from 'bull';
import * as Logger from 'bunyan';
import { EventEmitter } from 'events';
import Redis from 'redis';
const redis = require('redis');
const redisScan = require('node-redis-scan');
import { register as globalRegister, Registry } from 'prom-client';

import { logger as globalLogger } from './logger';
import { getJobCompleteStats, getStats, makeGuages, QueueGauges } from './queueGauges';

export interface MetricCollectorOptions extends Omit<bull.QueueOptions, 'redis'> {
  metricPrefix: string;
  autoDiscover: boolean;
  logger: Logger;
}

export interface QueueData<T = unknown> {
  queue: bull.Queue<T>;
  name: string;
  prefix: string;
}

export class MetricCollector {

  private readonly logger: Logger;
  private readonly redisClient: Redis.RedisClient;
  private readonly redisUri!: string;
  private readonly bullOpts!: Omit<bull.QueueOptions, 'redis'>;
  private readonly queuesByName: Map<string, QueueData<unknown>> = new Map();
  private get queues(): QueueData<unknown>[] {
    return [...this.queuesByName.values()];
  }
  private readonly myListeners: Set<(id: string) => Promise<void>> = new Set();
  private readonly guages!: QueueGauges;

  constructor(
    queueNames: string[],
    opts: MetricCollectorOptions,
    registers: Registry[] = [globalRegister],
  ) {
    
    const { logger, autoDiscover, metricPrefix, ...bullOpts } = opts;
    this.logger = logger || globalLogger;
    //this.redisUri = redis;
    this.logger.info("DEFAULT");
    //this.logger.info("REDIS HOST: ", process.env.REDIS_HOST);
    this.logger.info("REDIS CONNECTION: ", process.env.REDIS_CONNECTION);
    //this.logger.info("REDIS PORT: ", process.env.REDIS_PORT);
    //this.logger.info("REDIS PASSWORD: ", process.env.REDIS_PASSWORD);
    this.logger.info("REDIS CA: ", process.env.REDIS_CA_CERT);
    
    let ca = process.env.REDIS_CA_CERT;
    let tls = { ca };
    this.redisClient = redis.createClient(process.env.REDIS_CONNECTION, { tls });

    this.redisClient.setMaxListeners(32);
    this.bullOpts = bullOpts;
    this.addToQueueSet(queueNames);
    this.guages = makeGuages(metricPrefix, registers);

    this.logger.info("PINGING...", this.redisClient.ping());
    
  }

  private createClient(_type: 'client' | 'subscriber' | 'bclient'): any {
    if (_type === 'client') {
      return this.redisClient;
    }
    //this.logger.info("NOT DEFAULT");
    //this.logger.info("REDIS URI: ", this.redisUri);
    this.logger.info("REDIS CONNECTION: ", process.env.REDIS_CONNECTION);
    //this.logger.info("REDIS PORT: ", process.env.REDIS_PORT);
    //this.logger.info("REDIS PASSWORD: ", process.env.REDIS_PASSWORD);
    this.logger.info("REDIS CA: ", process.env.REDIS_CA_CERT);

    this.logger.info("REDIS URI: ", this.redisUri);
    let ca = process.env.REDIS_CA_CERT;
    let tls = { ca };
    return redis.createClient(process.env.REDIS_CONNECTION, { tls });
  }

  private addToQueueSet(names: string[]): void {
    for (const name of names) {
      if (this.queuesByName.has(name)) {
        continue;
      }
      this.logger.info('added queue', name);
      this.queuesByName.set(name, {
        name,
        queue: new bull(name, {
          ...this.bullOpts,
          createClient: this.createClient.bind(this),
        }),
        prefix: this.bullOpts.prefix || 'bull',
      });
    }
  }

  public async discoverAll(): Promise<void> {
    const keyPattern = new RegExp(`^${this.bullOpts.prefix}:([^:]+):(id|failed|active|waiting|stalled-check)$`);
    this.logger.info({ pattern: keyPattern.source }, 'running queue discovery');
    const scanner = new redisScan(this.redisClient);
    scanner.eachScan('bull:*:*', (matchingKeys: String[]) => {
      if(matchingKeys.length) {
        this.logger.info("KEY MATCH", matchingKeys);
      }
      this.logger.info("Empty match");
      
    });
    this.logger.info("Finished queue discovery");
    // const keyStream = await this.redisClient.scan("0", "MATCH", `${this.bullOpts.prefix}:*:*`, "COUNT", "1000");
    // // tslint:disable-next-line:await-promise tslint does not like Readable's here
    // for await (const keyChunk of keyStream) {
    //   for (const key of keyChunk) {
    //     const match = keyPattern.exec(key);
    //     if (match && match[1]) {
    //       this.addToQueueSet([match[1]]);
    //     }
    //   }
    // }
  }

  private async onJobComplete(queue: QueueData, id: string): Promise<void> {
    try {
      const job = await queue.queue.getJob(id);
      if (!job) {
        this.logger.warn({ job: id }, 'unable to find job from id');
        return;
      }
      await getJobCompleteStats(queue.prefix, queue.name, job, this.guages);
    } catch (err) {
      this.logger.error({ err, job: id }, 'unable to fetch completed job');
    }
  }

  public collectJobCompletions(): void {
    for (const q of this.queues) {
      const cb = this.onJobComplete.bind(this, q);
      this.myListeners.add(cb);
      q.queue.on('global:completed', cb);
    }
  }

  public async updateAll(): Promise<void> {
    const updatePromises = this.queues.map(q => getStats(q.prefix, q.name, q.queue, this.guages));
    await Promise.all(updatePromises);
  }

  public async ping(): Promise<void> {
    await this.redisClient.ping();
  }

  public async close(): Promise<void> {
    this.redisClient.end(true);
    for (const q of this.queues) {
      for (const l of this.myListeners) {
        (q.queue as any as EventEmitter).removeListener('global:completed', l);
      }
    }
    await Promise.all(this.queues.map(q => q.queue.close()));
  }

}
