import { PortalClient } from '@abernatskiy/portal-client';
import { Logger as PinoLogger, pino } from 'pino';
import { StateManager } from './state_manager';

type Logger = PinoLogger;

export type DatasourceOptions<Args extends {}> = {
  portal: PortalClient;
  stateManager?: StateManager;
  logger?: Logger;
} & Args;

export interface Datasource<T = unknown> {
  stream(): Promise<ReadableStream<T>>;
}

export abstract class AbstractDatasource<Args extends {}, Res = unknown>
  implements Datasource<Res> {
  constructor(public readonly options: DatasourceOptions<Args>) {
    if (!options.logger) {
      options.logger = pino();
    }
  }

  abstract stream(): Promise<ReadableStream<Res>>;
}
