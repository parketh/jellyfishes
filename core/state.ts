import { HashAndNumber } from '@abernatskiy/portal-client/src/query';

export interface State {
  set(...args: any[]): Promise<unknown>;

  get(): Promise<HashAndNumber | undefined>;
}
