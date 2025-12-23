require('dotenv').config(); // Carrega as variáveis do arquivo .env
const mqtt = require('mqtt');
const axios = require('axios');
const fs = require('fs');
const path = require('path');

// --- CONFIGURAÇÕES ---
// As variáveis abaixo são lidas do arquivo .env
const MQTT_URL = process.env.MQTT_URL;
const WEBHOOK_URL = process.env.WEBHOOK_URL;
const FOTOS_PATH = process.env.FOTOS_PATH || path.join(__dirname, 'fotos-inspecoes');

// Validação básica de segurança
if (!MQTT_URL || !WEBHOOK_URL) {
    console.error("❌ ERRO: MQTT_URL ou WEBHOOK_URL não definidos no arquivo .env");
    process.exit(1);
}

// Garante que a pasta de fotos exista no caminho especificado
if (!fs.existsSync(FOTOS_PATH)) {
    fs.mkdirSync(FOTOS_PATH, { recursive: true });
    console.log(`📁 Pasta de fotos configurada em: ${path.resolve(FOTOS_PATH)}`);
}

// Buffers temporários para conciliar as mensagens assíncronas
let bufferLeituras = null;
const bufferFotos = new Map(); // Armazena temporariamente id -> base64

// Configuração da conexão MQTT (Suporta SSL/TLS)
const client = mqtt.connect(MQTT_URL, { 
    rejectUnauthorized: false, // Necessário para alguns brokers com certificados auto-assinados
    connectTimeout: 10000 
});

client.on('connect', () => {
    console.log('✅ BFF Conectado ao Broker MQTT');
    client.subscribe('alcateia/teste/riodeserto/emcampo/leituras');
    client.subscribe('alcateia/teste/riodeserto/emcampo/fotos/#');
});

client.on('error', (err) => {
    console.error('❌ Erro na conexão MQTT:', err.message);
});

client.on('message', (topic, message) => {
    try {
        const payload = JSON.parse(message.toString());

        // Identifica se é o pacote de dados
        if (topic === 'alcateia/teste/riodeserto/emcampo/leituras') {
            console.log('📥 Lista de leituras recebida.');
            bufferLeituras = payload;
            reiniciarTimeout();
        } 
        // Identifica se é uma foto individual
        else if (topic.startsWith('alcateia/teste/riodeserto/emcampo/fotos/')) {
            const id = topic.split('/').pop();
            console.log(`📸 Foto recebida para o ID: ${id}`);
            bufferFotos.set(id, payload.fotoBase64);
            reiniciarTimeout();
        }
    } catch (e) {
        console.error("❌ Erro ao processar payload JSON:", e.message);
    }
});

let timeoutHandle = null;

/**
 * Função de Debounce:
 * Espera 2 segundos após a última mensagem recebida (seja foto ou dado)
 * para garantir que o pacote completo chegou antes de processar.
 */
function reiniciarTimeout() {
    if (timeoutHandle) clearTimeout(timeoutHandle);
    timeoutHandle = setTimeout(processarConciliacao, 2000);
}

async function processarConciliacao() {
    if (!bufferLeituras) return;

    console.log('🔄 Iniciando conciliação e salvamento físico...');
    
    const campo = bufferLeituras.Campo;
    const categorias = Object.keys(campo); // Varre LeituraCampoPP, PB, etc.

    categorias.forEach(cat => {
        if (campo[cat]) {
            campo[cat] = campo[cat].map(leitura => {
                // Verifica se temos uma foto em memória para esta leitura específica
                if (bufferFotos.has(leitura.id)) {
                    const base64Data = bufferFotos.get(leitura.id);
                    
                    // Extrai código do ponto (ex: PB-02) para o nome do arquivo
                    const codigoPonto = leitura.poco.split(' - ')[0].trim();
                    const nomeArquivo = `${codigoPonto} - ${leitura.id}.jpg`;
                    const caminhoCompleto = path.join(path.resolve(FOTOS_PATH), nomeArquivo);

                    try {
                        // Converte base64 para binário e grava no disco
                        fs.writeFileSync(caminhoCompleto, Buffer.from(base64Data, 'base64'));
                        
                        // Remove da memória imediatamente após salvar no disco
                        bufferFotos.delete(leitura.id);

                        // Injeta o caminho absoluto no objeto para o n8n
                        return { ...leitura, local_arquivo_foto: caminhoCompleto };
                    } catch (err) {
                        console.error(`❌ Erro crítico ao gravar arquivo ${leitura.id}:`, err.message);
                    }
                }
                return leitura; // Retorna a leitura (com ou sem o novo campo de caminho)
            });
        }
    });

    // Dispara o Webhook para o n8n com o objeto completo e caminhos locais
    try {
        await axios.post(WEBHOOK_URL, bufferLeituras);
        console.log('🚀 Objeto conciliado com sucesso e enviado ao n8n.');
        
        // Limpa o buffer de leituras para evitar duplicidade no próximo envio
        bufferLeituras = null;
    } catch (error) {
        console.error('❌ Erro ao enviar para o n8n:', error.message);
    }
}