# Usa uma imagem leve do Node.js
FROM node:18-alpine

# Define que a aplicação rodará em modo de produção
ENV NODE_ENV=production

# Define o diretório de trabalho principal para o código
WORKDIR /usr/src/app

# Copia apenas os arquivos de dependências para otimizar o cache das camadas
COPY package*.json ./

# Instala apenas as dependências de produção (ignora nodemon, etc)
RUN npm install --omit=dev

# Copia o restante dos arquivos do projeto (index.js, etc)
COPY . .

# Comando para iniciar a aplicação
CMD [ "node", "index.js" ]



# docker run -d \
#   --name leitura-piezometros-ap \
#   -p 3000:3000 \
#   -v /home/usuario/fotos-inspecoes:/fotos-inspecoes \
#   --env-file .env \
#   leitura-piezometros-ap