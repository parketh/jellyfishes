import { PortalClient } from '@abernatskiy/portal-client';
import { Logger as PinoLogger, pino } from 'pino';
import { State } from './state';

type Logger = PinoLogger;

export type DatasourceOptions<Args extends {}> = {
  portal: PortalClient;
  state?: State;
  logger?: Logger;
} & Args;

export interface Datasource<T = unknown> {
  stream(): Promise<ReadableStream<T>>;
}

export abstract class AbstractDatasource<Args extends {}, Res = unknown>
  implements Datasource<Res> {
  logger: Logger;

  constructor(public readonly options: DatasourceOptions<Args>) {
    this.logger = options.logger || pino();
  }

  abstract stream(): Promise<ReadableStream<Res>>;
}
