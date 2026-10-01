FROM node:22-alpine
WORKDIR /app
COPY package.json package-lock.json tsconfig.json ./
RUN npm ci
COPY src ./src
COPY server ./server
COPY cli ./cli
COPY test ./test
COPY web ./web
RUN npx tsc -p .
ENV PORT=7000 DATA_DIR=/data
EXPOSE 7000
CMD ["node", "build/server/node.js"]
