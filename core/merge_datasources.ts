import { Datasource } from './abstract_datasource';

export function mergeConcurrently<T extends Datasource[]>(
  ...dataSources: T
): Datasource<{
  [K in keyof T]: T[K]['stream'] extends () => Promise<ReadableStream<infer U>> ? U : never;
}> {
  return {
    stream: async () => {
      return new ReadableStream({
        async start(controller) {
          const streams = await Promise.all(dataSources.map((s) => s.stream()));
          let activeReaders = streams.length;

          for (let i = 0; i < streams.length; i++) {
            async function readFromStream() {
              const reader = streams[i].getReader();

              while (true) {
                const {done, value} = await reader.read();
                if (done) {
                  activeReaders--;
                  if (activeReaders === 0) controller.close();
                  break;
                }

                const res = new Array(streams.length).fill([]);
                res[i] = value;

                controller.enqueue(res as any);
              }
            }

            void readFromStream();
          }
        },
      });
    },
  };
}
