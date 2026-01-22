require('dotenv').config(); 
const mqtt = require('mqtt');
const axios = require('axios');
const fs = require('fs');
const path = require('path');
const Firebird = require('node-firebird');
const iconv = require('iconv-lite'); 
const { Pool } = require('pg');
const cron = require('node-cron');
const express = require('express'); // Adicionado Express

const app = express();
const PORTA_API = process.env.PORTA_CONTAINER || 3001;
const IP_VPS = process.env.IP_VPS || '192.168.100.95'; // Configure seu IP no .env

// --- CONFIGURAÇÕES ---
const WEBHOOK_URL = process.env.WEBHOOK_URL || 'http://192.168.100.95:5678/webhook/leituras-aplicativo';
const FOTOS_PATH = '/fotos-inspecoes';
const MQTT_TOPIC_BASE = process.env.MQTT_TOPIC_RESULTADO || 'alcateia/teste/riodeserto/lista/piezometro'; 

// --- SERVIDOR WEB (EXPOSIÇÃO DE FOTOS) ---
// Isso permite acessar: http://IP_DA_VPS:3000/ver-fotos/ID_DA_FOTO.jpg
app.use('/ver-fotos', express.static(FOTOS_PATH));

app.get('/status', (req, res) => {
    res.json({ status: "online", fotos_path: FOTOS_PATH });
});

app.listen(PORTA_API, '0.0.0.0', () => {
    console.log(`--------------------------------------------------`);
    console.log(`🌐 Servidor Web Ativo na porta ${PORTA_API}`);
    console.log(`📸 Fotos acessíveis em: http://${IP_VPS}:${PORTA_API}/ver-fotos/`);
    console.log(`--------------------------------------------------`);
});

// Configuração Firebird
const dbOptionsFirebird = {
    host: process.env.DB_HOST,
    port: process.env.DB_PORT,
    database: process.env.DB_DATABASE,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    lowercase_keys: false, 
    role: null,            
    pageSize: 4096,
    charset: 'NONE'
};

// Configuração PostgreSQL
const pgPool = new Pool({
    host: process.env.PG_HOST,
    port: process.env.PG_PORT,
    database: process.env.PG_DATABASE,
    user: process.env.PG_USER,
    password: process.env.PG_PASSWORD,
    max: 10, 
    idleTimeoutMillis: 30000
});

// Configuração MQTT
const mqttOptions = {
    host: process.env.MQTT_HOST || 'broker.hivemq.com',
    port: process.env.MQTT_PORT || 8883,
    protocol: 'mqtts',
    username: process.env.MQTT_USER || '',
    password: process.env.MQTT_PASSWORD || '',
    connectTimeout: 10000,
    rejectUnauthorized: false 
};

// --- FUNÇÃO AUXILIAR DECODIFICAÇÃO ---
function decodificarBuffer(valor) {
    if (!valor) return null;
    if (Buffer.isBuffer(valor)) return iconv.decode(valor, 'win1252').trim();
    if (typeof valor === 'string') return valor.trim();
    return valor;
}

// --- SETUP INICIAL ---
if (!fs.existsSync(FOTOS_PATH)) {
    fs.mkdirSync(FOTOS_PATH, { recursive: true });
}

// Buffers temporários
let bufferLeituras = null;
const bufferFotos = new Map();

// --- CONEXÃO MQTT ---
console.log(`🔌 Conectando ao MQTT...`);
const client = mqtt.connect(mqttOptions);

client.on('connect', () => {
    console.log('✅ Conectado ao Broker MQTT (SSL)');
    client.subscribe('alcateia/teste/riodeserto/emcampo/leituras');
    client.subscribe('alcateia/teste/riodeserto/emcampo/fotos/#');
    consultarBancoPublicarMQTT();

    cron.schedule('0 6 * * *', () => {
        consultarBancoPublicarMQTT();
    }, { scheduled: true, timezone: "America/Sao_Paulo" });
});

client.on('message', (topic, message) => {
    if (topic.startsWith(MQTT_TOPIC_BASE)) return;
    try {
        const payload = JSON.parse(message.toString());
        if (topic.includes('leituras')) { 
            bufferLeituras = payload; 
            reiniciarTimeout(); 
        }
        else if (topic.includes('fotos')) { 
            const id = topic.split('/').pop();
            console.log(`📸 Foto recebida (ID: ${id})`);
            bufferFotos.set(id, payload.fotoBase64); 
            reiniciarTimeout(); 
        }
    } catch (e) {
        console.error("❌ Erro payload MQTT:", e.message);
    }
});

let timeoutHandle = null;
function reiniciarTimeout() {
    if (timeoutHandle) clearTimeout(timeoutHandle);
    timeoutHandle = setTimeout(processarConciliacao, 2000);
}

async function processarConciliacao() {
    if (!bufferLeituras) return;

    console.log('🔄 Iniciando conciliação e salvamento físico...');
    const campo = bufferLeituras.Campo;
    const categorias = Object.keys(campo);

    for (const cat of categorias) {
        if (campo[cat]) {
            const leiturasAtualizadas = [];
            for (const leitura of campo[cat]) {
                let urlFotoFinal = null;
                const leituraId = String(leitura.id);
                const nomeArquivo = `${leituraId}.jpg`;
                const caminhoCompleto = path.join(FOTOS_PATH, nomeArquivo);
                
                if (bufferFotos.has(leituraId)) {
                    const base64Data = bufferFotos.get(leituraId);

                    try {
                        fs.writeFileSync(caminhoCompleto, Buffer.from(base64Data, 'base64'));
                        bufferFotos.delete(leituraId);
                        
                        // Caminho Web para ser usado no Webhook e Frontend
                        urlFotoFinal = `http://${IP_VPS}:${PORTA_API}/ver-fotos/${nomeArquivo}`;
                        
                        console.log(`💾 ARQUIVO SALVO: ${caminhoCompleto}`);
                        console.log(`🔗 URL GERADA: ${urlFotoFinal}`);

                        await pgPool.query(
                            `INSERT INTO TB_FOTO_INSPECAO (CD_PIEZOMETRO, NM_ARQUIVO, CAMINHO_COMPLETO) VALUES ($1, $2, $3)`,
                            [leitura.CD_PIEZOMETRO || 0, nomeArquivo, urlFotoFinal]
                        );
                    } catch (err) { 
                        console.error('❌ Erro ao salvar:', err.message); 
                    }
                } else if (fs.existsSync(caminhoCompleto)) {
                    // Se a foto já existe no disco, gera a URL mesmo sem estar no buffer
                    urlFotoFinal = `http://${IP_VPS}:${PORTA_API}/ver-fotos/${nomeArquivo}`;
                }

                const { foto, ...leituraLimpa } = leitura;
                leiturasAtualizadas.push({ 
                    ...leituraLimpa, 
                    caminho_imagem: urlFotoFinal || leitura.caminho_imagem || null
                });
            }
            campo[cat] = leiturasAtualizadas;
        }
    }

    try {
        await axios.post(WEBHOOK_URL, bufferLeituras);
        console.log('🚀 Webhook (n8n) enviado com sucesso.');
        bufferLeituras = null; 
    } catch (error) { 
        console.error('❌ Erro Webhook:', error.message); 
    }
}

function consultarBancoPublicarMQTT() {
    const sqlQuery = `
        SELECT 
            P.CD_PIEZOMETRO, 
            CAST(P.ID_PIEZOMETRO AS VARCHAR(200) CHARACTER SET OCTETS) AS ID_RAW,
            CAST(P.NM_PIEZOMETRO AS VARCHAR(1024) CHARACTER SET OCTETS) AS NM_RAW,
            CAST(P.TP_PIEZOMETRO AS VARCHAR(50) CHARACTER SET OCTETS) AS TP_RAW,
            CAST(P.FG_SITUACAO AS VARCHAR(20) CHARACTER SET OCTETS) AS FG_RAW,
            P.CD_EMPRESA,
            MAX(I.DT_INSPECAO) AS DT_INSPECAO,
            MAX(I.QT_LEITURA) AS QT_LEITURA
        FROM 
            TB_PIEZOMETRO P
        LEFT JOIN 
            TB_INSPECAO_PIEZOMETRO_MVTO I 
            ON I.CD_INSPECAO_PIEZOMETRO = P.CD_PIEZOMETRO
            AND I.DT_INSPECAO = (
                SELECT MAX(SUB.DT_INSPECAO)
                FROM TB_INSPECAO_PIEZOMETRO_MVTO SUB
                WHERE SUB.CD_INSPECAO_PIEZOMETRO = P.CD_PIEZOMETRO
            )
        WHERE 
            P.FG_SITUACAO = 'A' 
            AND P.CD_EMPRESA = '18'
        GROUP BY 
            P.CD_PIEZOMETRO, P.ID_PIEZOMETRO, P.NM_PIEZOMETRO,
            P.TP_PIEZOMETRO, P.FG_SITUACAO, P.CD_EMPRESA
    `;

    Firebird.attach(dbOptionsFirebird, (err, db) => {
        if (err) return;
        db.query(sqlQuery, (err, result) => {
            db.detach(); 
            if (err || !result) return;
            const dadosAgrupados = {};
            result.forEach(row => {
                const objetoLimpo = {
                    CD_PIEZOMETRO: row.CD_PIEZOMETRO,
                    ID_PIEZOMETRO: decodificarBuffer(row.ID_RAW),
                    NM_PIEZOMETRO: decodificarBuffer(row.NM_RAW),
                    TP_PIEZOMETRO: decodificarBuffer(row.TP_RAW),
                    CD_EMPRESA: row.CD_EMPRESA,
                    FG_SITUACAO: decodificarBuffer(row.FG_RAW),
                    DT_INSPECAO: row.DT_INSPECAO,
                    QT_LEITURA: row.QT_LEITURA
                };
                const tipo = objetoLimpo.TP_PIEZOMETRO || 'OUTROS';
                if (!dadosAgrupados[tipo]) dadosAgrupados[tipo] = [];
                dadosAgrupados[tipo].push(objetoLimpo);
            });
            Object.keys(dadosAgrupados).forEach(tipo => {
                client.publish(`${MQTT_TOPIC_BASE}/${tipo}`, JSON.stringify(dadosAgrupados[tipo]), { retain: true, qos: 1 });
            });
        });
    });
}