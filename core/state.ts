import { BlockRef } from './abstract_stream';

export interface State {
  set(...args: any[]): Promise<unknown>;

  get(): Promise<BlockRef | undefined>;
}
