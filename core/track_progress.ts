import { PortalClient } from '@subsquid/portal-client';
import { Throttler } from '@subsquid/util-internal';
import { BlockRef } from './abstract_stream';

type Progress = {
  blocks: {
    head: number;
    current: number;
    percent_done: number;
  };
  interval: {
    processed: number;
    speed: string;
  };
};

export type TrackProgressOptions = {
  portal: PortalClient;
  intervalSeconds?: number;
  onProgress: (progress: Progress) => void;
};

export class TrackProgress {
  last: { block: BlockRef; ts: number };
  current: BlockRef;

  constructor({portal, intervalSeconds = 10, onProgress}: TrackProgressOptions) {
    const headCall = new Throttler(() => portal.getFinalizedHeight(), 60_000);

    setInterval(async () => {
      const head = await headCall.get();
      const processed = this.last ? this.current.number - this.last.block.number : 0;
      const elapsed = this.last ? (Date.now() - this.last.ts) / 1000 : 0;
      const speed =
        processed && elapsed ? `${Math.floor(processed / elapsed)} blocks/sec` : 'unknown';

      onProgress({
        blocks: {
          head,
          current: this.current.number,
          percent_done: (this.current.number / head) * 100,
        },
        interval: {
          processed,
          speed,
        },
      });

      this.last = {block: this.current, ts: Date.now()};
    }, intervalSeconds * 1000);
  }

  track(block: BlockRef) {
    this.current = block;
  }
}
