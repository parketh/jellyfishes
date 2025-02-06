export interface State {
  set(...args: any[]): Promise<unknown>;

  get(): Promise<number | undefined>;
}
