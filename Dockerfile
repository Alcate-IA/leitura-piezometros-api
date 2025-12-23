# Usa uma imagem leve do Node
FROM node:18-alpine

# Define ambiente de produção
ENV NODE_ENV=production

# Diretório de trabalho
WORKDIR /usr/src/app

# Copia arquivos de dependências
COPY package*.json ./

# Instala apenas o necessário para produção
RUN npm install --omit=dev

# Copia o código da API
COPY . .

# Cria a pasta de fotos interna
RUN mkdir -p /imagens_inspecoes

# Define a variável de ambiente para o código usar essa pasta
ENV FOTOS_PATH=/imagens_inspecoes

# Expõe o volume para persistência no host
VOLUME ["/imagens_inspecoes"]

# Comando de inicialização
CMD [ "node", "index.js" ]