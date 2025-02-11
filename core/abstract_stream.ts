import { PortalClient } from '@subsquid/portal-client';
import { Logger as PinoLogger, pino } from 'pino';
import { State } from './state';

type Logger = PinoLogger;

export type BlockRef = {
  number: number;
  hash: string;
};

export type StreamOptions<Args extends {}> = {
  portal: PortalClient;
  state?: State;
  logger?: Logger;
} & Args;

export interface Stream<T = unknown> {
  stream(): Promise<ReadableStream<T>>;
}

export abstract class AbstractStream<Args extends {}, Res = unknown> implements Stream<Res> {
  logger: Logger;

  constructor(public readonly options: StreamOptions<Args>) {
    this.logger =
      options.logger ||
      pino({base: null, messageKey: 'message', level: process.env.LOG_LEVEL || 'info'});
  }

  abstract stream(): Promise<ReadableStream<Res>>;
}
