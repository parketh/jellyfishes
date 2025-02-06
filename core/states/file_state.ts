import fs from 'node:fs/promises';
import { HashAndNumber } from '@abernatskiy/portal-client/src/query';
import { State } from '../state';

export class FileState implements State {
  constructor(private readonly filepath: string) {
  }

  async set(state: HashAndNumber) {
    await fs.writeFile(this.filepath, state.toString());
  }

  async get() {
    try {
      const state = await fs.readFile(this.filepath, 'utf8');
      if (state) return parseInt(state, 10);
    } catch (e: any) {
      if (e.code !== 'ENOENT') {
        throw e;
      }
    }

    return;
  }
}
