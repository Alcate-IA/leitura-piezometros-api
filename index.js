require('dotenv').config(); 
const mqtt = require('mqtt');
const axios = require('axios');
const fs = require('fs');
const path = require('path');
const Firebird = require('node-firebird');
const iconv = require('iconv-lite'); 
const { Pool } = require('pg');
const cron = require('node-cron');

// --- CONFIGURAÇÕES ---
const WEBHOOK_URL = process.env.WEBHOOK_URL || 'http://192.168.100.95:5678/webhook/leituras-aplicativo';
const FOTOS_PATH = process.env.FOTOS_PATH || '/fotos-inspecoes';
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
if (!fs.existsSync(FOTOS_PATH)) {
    fs.mkdirSync(FOTOS_PATH, { recursive: true });
    console.log(`📁 Pasta de fotos configurada em: ${path.resolve(FOTOS_PATH)}`);
}

// Buffers temporários
let bufferLeituras = null;
const bufferFotos = new Map();

// --- CONEXÃO MQTT ---
console.log(`🔌 Conectando ao MQTT...`);
const client = mqtt.connect(mqttOptions);

client.on('connect', () => {
    console.log('✅ Conectado ao Broker MQTT (SSL)');
    
    // Inscrição nos tópicos de entrada (Android -> BFF)
    client.subscribe('alcateia/teste/riodeserto/emcampo/leituras');
    client.subscribe('alcateia/teste/riodeserto/emcampo/fotos/#');

    // Carga inicial de dados do banco para o aplicativo
    console.log('🚀 Executando carga inicial de dados...');
    consultarBancoPublicarMQTT();

    // Agenda para rodar todo dia às 06:00h
    cron.schedule('0 6 * * *', () => {
        console.log('⏰ Executando agendamento diário (06:00h)...');
        consultarBancoPublicarMQTT();
    }, {
        scheduled: true,
        timezone: "America/Sao_Paulo"
    });
});

client.on('message', (topic, message) => {
    // Evita processar mensagens que o próprio BFF publicou
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
            console.log(`📸 Foto recebida para o ID: ${id}`);
            bufferFotos.set(id, payload.fotoBase64); 
            reiniciarTimeout(); 
        }
    } catch (e) {
        console.error("❌ Erro ao processar payload MQTT:", e.message);
    }
});

// --- LÓGICA DE CONCILIAÇÃO (DEBOUNCE) ---
let timeoutHandle = null;
function reiniciarTimeout() {
    if (timeoutHandle) clearTimeout(timeoutHandle);
    timeoutHandle = setTimeout(processarConciliacao, 2000);
}

async function processarConciliacao() {
    if (!bufferLeituras) return;

    console.log('🔄 Iniciando conciliação, salvamento físico e banco...');
    
    const campo = bufferLeituras.Campo;
    const categorias = Object.keys(campo);

    for (const cat of categorias) {
        if (campo[cat]) {
            const leiturasAtualizadas = [];
            
            for (const leitura of campo[cat]) {
                let caminhoFotoFinal = null;
                
                // 1. Processamento da Foto (se existir no buffer)
                if (bufferFotos.has(leitura.id)) {
                    const base64Data = bufferFotos.get(leitura.id);
                    const nomeArquivo = `${leitura.id}.jpg`;
                    const caminhoCompleto = path.join(path.resolve(FOTOS_PATH), nomeArquivo);

                    try {
                        // Grava arquivo no disco
                        fs.writeFileSync(caminhoCompleto, Buffer.from(base64Data, 'base64'));
                        bufferFotos.delete(leitura.id);
                        caminhoFotoFinal = caminhoCompleto;

                        // Salva registro da foto no Postgres
                        const cdPiezometroSalvar = leitura.CD_PIEZOMETRO || 0;
                        try {
                            await pgPool.query(
                                `INSERT INTO TB_FOTO_INSPECAO (CD_PIEZOMETRO, NM_ARQUIVO, CAMINHO_COMPLETO) VALUES ($1, $2, $3)`,
                                [cdPiezometroSalvar, nomeArquivo, caminhoCompleto]
                            );
                            console.log(`💾 Foto registrada no Postgres para CD: ${cdPiezometroSalvar}`);
                        } catch (pgErr) { 
                            console.error('❌ Erro Postgres:', pgErr.message); 
                        }

                    } catch (err) { 
                        console.error('❌ Erro ao salvar arquivo:', err.message); 
                    }
                }

                // 2. Ajuste do Objeto para o Webhook
                // Removemos o campo 'foto' (base64) e tratamos a observação como string simples
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

    // Envia o objeto final para o n8n
    try {
        await axios.post(WEBHOOK_URL, bufferLeituras);
        console.log('🚀 Webhook enviado com sucesso.');
        bufferLeituras = null; // Limpa para a próxima carga
    } catch (error) { 
        console.error('❌ Erro ao enviar para o Webhook:', error.message); 
    }
}

// --- BANCO DE DADOS (FIREBIRD -> MQTT) ---
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

            if (err) { console.error('❌ Erro Query Firebird:', err.message); return; }

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
                    console.log(`📡 Publicado: ${topico} (${dadosAgrupados[tipo].length} itens)`);
                });
            } else {
                console.log('⚠️ Consulta Firebird vazia.');
            }
        });
    });
}