FROM node:22

COPY . .

RUN corepack enable && yarn install && yarn global add ts-node

ENTRYPOINT ["bash", "-c"]




# docker build -t mo4islona/sqdgn-pipes:latest .