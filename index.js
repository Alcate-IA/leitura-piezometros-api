require('dotenv').config(); 
const mqtt = require('mqtt');
const axios = require('axios');
const fs = require('fs');
const path = require('path');
const Firebird = require('node-firebird');
const iconv = require('iconv-lite'); 
const { Pool } = require('pg');
const cron = require('node-cron');

app.use('/ver-fotos', express.static(FOTOS_PATH));

// --- CONFIGURAÇÕES ---
const WEBHOOK_URL = process.env.WEBHOOK_URL || 'http://192.168.100.95:5678/webhook/leituras-aplicativo';

/**
 * FORÇANDO CAMINHO ABSOLUTO PARA O VOLUME
 * O '/' no início é crucial para que o Node procure na raiz do container,
 * onde o volume da VPS está montado.
 */
const FOTOS_PATH = '/fotos-inspecoes';

const MQTT_TOPIC_BASE = process.env.MQTT_TOPIC_RESULTADO || 'alcateia/teste/riodeserto/lista/piezometro'; 

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
console.log("--- CONFIGURAÇÃO DE DIRETÓRIOS ---");
if (!fs.existsSync(FOTOS_PATH)) {
    fs.mkdirSync(FOTOS_PATH, { recursive: true });
    console.log(`📁 Pasta criada na raiz: ${FOTOS_PATH}`);
}
console.log(`📍 O sistema salvará fotos em: ${path.resolve(FOTOS_PATH)}`);
console.log("----------------------------------");

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

    console.log('🚀 Executando carga inicial de dados...');
    consultarBancoPublicarMQTT();

    cron.schedule('0 6 * * *', () => {
        console.log('⏰ Executando agendamento diário (06:00h)...');
        consultarBancoPublicarMQTT();
    }, {
        scheduled: true,
        timezone: "America/Sao_Paulo"
    });
});

client.on('message', (topic, message) => {
    if (topic.startsWith(MQTT_TOPIC_BASE)) return;

    try {
        const payload = JSON.parse(message.toString());
        
        if (topic.includes('leituras')) { 
            console.log('📥 Lista de leituras recebida.');
            bufferLeituras = payload; 
            reiniciarTimeout(); 
        }
        else if (topic.includes('fotos')) { 
            const id = topic.split('/').pop();
            console.log(`📸 Foto recebida (ID: ${id}) - Tamanho Base64: ${payload.fotoBase64?.length || 0}`);
            bufferFotos.set(id, payload.fotoBase64); 
            reiniciarTimeout(); 
        }
    } catch (e) {
        console.error("❌ Erro ao processar payload MQTT:", e.message);
    }
});

let timeoutHandle = null;
function reiniciarTimeout() {
    if (timeoutHandle) clearTimeout(timeoutHandle);
    timeoutHandle = setTimeout(processarConciliacao, 2000);
}

async function processarConciliacao() {
    if (!bufferLeituras) {
        console.log("⏳ Aguardando lista de leituras para conciliar fotos...");
        return;
    }

    console.log('🔄 Iniciando processamento de arquivos no volume...');
    
    const campo = bufferLeituras.Campo;
    const categorias = Object.keys(campo);

    for (const cat of categorias) {
        if (campo[cat]) {
            const leiturasAtualizadas = [];
            
            for (const leitura of campo[cat]) {
                let caminhoFotoFinal = null;
                
                if (bufferFotos.has(leitura.id)) {
                    const base64Data = bufferFotos.get(leitura.id);
                    const nomeArquivo = `${leitura.id}.jpg`;
                    const caminhoCompleto = path.join(FOTOS_PATH, nomeArquivo);

                    try {
                        fs.writeFileSync(caminhoCompleto, Buffer.from(base64Data, 'base64'));
                        bufferFotos.delete(leitura.id);
                        caminhoFotoFinal = caminhoCompleto;
                        
                        console.log(`💾 ARQUIVO GRAVADO: ${caminhoCompleto}`);

                        const cdPiezometroSalvar = leitura.CD_PIEZOMETRO || 0;
                        try {
                            await pgPool.query(
                                `INSERT INTO TB_FOTO_INSPECAO (CD_PIEZOMETRO, NM_ARQUIVO, CAMINHO_COMPLETO) VALUES ($1, $2, $3)`,
                                [cdPiezometroSalvar, nomeArquivo, caminhoCompleto]
                            );
                            console.log(`✅ Registro DB OK (CD: ${cdPiezometroSalvar})`);
                        } catch (pgErr) { 
                            console.error('❌ Erro Postgres:', pgErr.message); 
                        }

                    } catch (err) { 
                        console.error('❌ ERRO AO ESCREVER NO VOLUME:', err.message); 
                    }
                }

                const { foto, ...leituraLimpa } = leitura;

                leiturasAtualizadas.push({ 
                    ...leituraLimpa, 
                    observacao: leitura.observacao && leitura.observacao.trim() !== "" ? leitura.observacao : null,
                    caminho_imagem: caminhoFotoFinal 
                });
            }
            campo[cat] = leiturasAtualizadas;
        }
    }

    try {
        await axios.post(WEBHOOK_URL, bufferLeituras);
        console.log('🚀 Webhook (n8n) enviado.');
        bufferLeituras = null; 
    } catch (error) { 
        console.error('❌ Erro Webhook:', error.message); 
    }
}

// --- FUNÇÃO CONSULTA BANCO ---
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
            P.CD_PIEZOMETRO, 
            P.ID_PIEZOMETRO, 
            P.NM_PIEZOMETRO,
            P.TP_PIEZOMETRO,
            P.FG_SITUACAO, 
            P.CD_EMPRESA
    `;

    Firebird.attach(dbOptionsFirebird, (err, db) => {
        if (err) { console.error('❌ Erro Firebird:', err.message); return; }

        db.query(sqlQuery, (err, result) => {
            db.detach(); 
            if (err) return;

            if (result && result.length > 0) {
                const dadosAgrupados = {};

                result.forEach(row => {
                    const idDecodificado = decodificarBuffer(row.ID_RAW);
                    const nmDecodificado = decodificarBuffer(row.NM_RAW);
                    const tpDecodificado = decodificarBuffer(row.TP_RAW);
                    const fgDecodificado = decodificarBuffer(row.FG_RAW);

                    const objetoLimpo = {
                        CD_PIEZOMETRO: row.CD_PIEZOMETRO,
                        ID_PIEZOMETRO: idDecodificado,
                        NM_PIEZOMETRO: nmDecodificado,
                        TP_PIEZOMETRO: tpDecodificado,
                        CD_EMPRESA: row.CD_EMPRESA,
                        FG_SITUACAO: fgDecodificado,
                        DT_INSPECAO: row.DT_INSPECAO,
                        QT_LEITURA: row.QT_LEITURA
                    };

                    const tipo = tpDecodificado || 'OUTROS';
                    if (!dadosAgrupados[tipo]) dadosAgrupados[tipo] = [];
                    dadosAgrupados[tipo].push(objetoLimpo);
                });

                Object.keys(dadosAgrupados).forEach(tipo => {
                    const topico = `${MQTT_TOPIC_BASE}/${tipo}`;
                    client.publish(topico, JSON.stringify(dadosAgrupados[tipo]), { retain: true, qos: 1 });
                });
            }
        });
    });
}