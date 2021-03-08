FROM node:lts-alpine

COPY package.json .

RUN npm install

COPY index.js .

CMD ["node", "./index.js"]