require('dotenv').config(); 
const mqtt = require('mqtt');
const axios = require('axios');
const fs = require('fs');
const path = require('path');

const MQTT_URL = process.env.MQTT_URL;
const WEBHOOK_URL = process.env.WEBHOOK_URL;
const FOTOS_PATH = process.env.FOTOS_PATH || path.join(__dirname, 'fotos-inspecoes');

if (!MQTT_URL || !WEBHOOK_URL) {
    console.error("❌ ERRO: MQTT_URL ou WEBHOOK_URL não definidos no arquivo .env");
    process.exit(1);
}

if (!fs.existsSync(FOTOS_PATH)) {
    fs.mkdirSync(FOTOS_PATH, { recursive: true });
    console.log(`📁 Pasta de fotos configurada em: ${path.resolve(FOTOS_PATH)}`);
}

let bufferLeituras = null;
const bufferFotos = new Map();

const client = mqtt.connect(MQTT_URL, { 
    rejectUnauthorized: false,
    connectTimeout: 10000 
});

client.on('connect', () => {
    console.log('✅ BFF Conectado ao Broker MQTT');
    client.subscribe('alcateia/teste/riodeserto/emcampo/leituras');
    client.subscribe('alcateia/teste/riodeserto/emcampo/fotos/#');
});

client.on('message', (topic, message) => {
    try {
        const payload = JSON.parse(message.toString());
        if (topic === 'alcateia/teste/riodeserto/emcampo/leituras') {
            bufferLeituras = payload;
            reiniciarTimeout();
        } 
        else if (topic.startsWith('alcateia/teste/riodeserto/emcampo/fotos/')) {
            const id = topic.split('/').pop();
            bufferFotos.set(id, payload.fotoBase64);
            reiniciarTimeout();
        }
    } catch (e) {
        console.error("❌ Erro ao processar payload JSON:", e.message);
    }
});

let timeoutHandle = null;

function reiniciarTimeout() {
    if (timeoutHandle) clearTimeout(timeoutHandle);
    timeoutHandle = setTimeout(processarConciliacao, 2000);
}

async function processarConciliacao() {
    if (!bufferLeituras) return;

    console.log('🔄 Conciliando dados e formatando campo observacao...');
    
    const campo = bufferLeituras.Campo;
    const categorias = Object.keys(campo);

    categorias.forEach(cat => {
        if (campo[cat]) {
            campo[cat] = campo[cat].map(leitura => {
                let caminhoFotoFinal = null;

                // 1. Processamento e Salvamento da Imagem
                if (bufferFotos.has(leitura.id)) {
                    const base64Data = bufferFotos.get(leitura.id);
                    const codigoPonto = leitura.poco.split(' - ')[0].trim();
                    const nomeArquivo = `${codigoPonto} - ${leitura.id}.jpg`;
                    const caminhoCompleto = path.join(path.resolve(FOTOS_PATH), nomeArquivo);

                    try {
                        fs.writeFileSync(caminhoCompleto, Buffer.from(base64Data, 'base64'));
                        bufferFotos.delete(leitura.id);
                        caminhoFotoFinal = caminhoCompleto;
                    } catch (err) {
                        console.error(`❌ Erro ao gravar foto ${leitura.id}:`, err.message);
                    }
                }

                // 2. Montagem do Objeto para o campo observacao
                const infoAdicional = {
                    comentario: leitura.observacao && leitura.observacao.trim() !== "" ? leitura.observacao : null,
                    url: caminhoFotoFinal 
                };

                // Retornamos o objeto sem o campo extra 'local_arquivo_foto'
                return { 
                    ...leitura, 
                    observacao: JSON.stringify(infoAdicional)
                };
            });
        }
    });

    try {
        await axios.post(WEBHOOK_URL, bufferLeituras);
        console.log('🚀 Dados enviados ao n8n (Observação processada)');
        bufferLeituras = null;
    } catch (error) {
        console.error('❌ Erro ao enviar para o n8n:', error.message);
    }
}